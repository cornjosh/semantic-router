"""Loopback-only model server for evaluation, never a production generation API.

Run in an environment with torch/transformers/aiohttp. Weights are downloaded
separately from ModelScope; loading is offline, safetensors-only, no remote code.
"""

import argparse
import asyncio
import json
from pathlib import Path

import torch
from aiohttp import web
from transformers import AutoModel, AutoModelForCausalLM, AutoTokenizer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--protocol", choices=["embedding", "llm"], default="embedding")
    parser.add_argument("--port", type=int, default=18089)
    parser.add_argument("--device", default="cuda")
    args = parser.parse_args()
    torch.set_num_threads(4)
    torch.manual_seed(42)
    options = dict(local_files_only=True, trust_remote_code=False)
    tokenizer = AutoTokenizer.from_pretrained(args.model_path, **options)
    cls = AutoModel if args.protocol == "embedding" else AutoModelForCausalLM
    model = cls.from_pretrained(args.model_path, use_safetensors=True, **options)
    model = model.to(args.device).eval()
    lock = asyncio.Lock()
    counts = {"requests": 0, "texts": 0}

    def infer(body):
        if body.get("model") != args.model_id:
            raise ValueError("unknown model")
        if args.protocol == "embedding":
            texts = body["input"]
            if not isinstance(texts, list) or not 1 <= len(texts) <= 1025:
                raise ValueError("invalid input count")
            output = []
            for start in range(0, len(texts), 64):
                batch = tokenizer(
                    texts[start : start + 64],
                    padding=True,
                    truncation=False,
                    return_tensors="pt",
                ).to(args.device)
                if batch.input_ids.shape[1] > 512:
                    raise ValueError("input exceeds 512 tokens; no silent truncation")
                with torch.inference_mode():
                    vectors = model(**batch).last_hidden_state[:, 0].float()
                    vectors = torch.nn.functional.normalize(vectors, p=2, dim=1)
                output.extend(vectors.cpu().tolist())
            counts["texts"] += len(texts)
            return {
                "model": args.model_id,
                "data": [
                    {"index": i, "embedding": vector} for i, vector in enumerate(output)
                ],
            }
        # No constrained-decoding repair: malformed model output is measured as
        # an adapter failure, never silently converted into a valid prediction.
        text = tokenizer.apply_chat_template(
            body["messages"],
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=False,
        )
        batch = tokenizer(text, return_tensors="pt").to(args.device)
        if batch.input_ids.shape[1] > 8192:
            raise ValueError("classification prompt exceeds evaluation budget")
        with torch.inference_mode():
            output = model.generate(
                **batch,
                do_sample=False,
                max_new_tokens=min(body["max_completion_tokens"], 256),
            )
        generated = output[0, batch.input_ids.shape[1] :]
        eos = model.generation_config.eos_token_id
        eos = [eos] if isinstance(eos, int) else eos
        finish = "stop" if generated[-1].item() in eos else "length"
        counts["texts"] += 1
        return {
            "model": args.model_id,
            "choices": [
                {
                    "finish_reason": finish,
                    "message": {
                        "content": tokenizer.decode(generated, skip_special_tokens=True)
                    },
                }
            ],
        }

    async def handle(request):
        body = await request.json()
        try:
            async with lock:
                result = await asyncio.to_thread(infer, body)
                counts["requests"] += 1
            return web.Response(
                text=json.dumps(result, separators=(",", ":")),
                content_type="application/json",
            )
        except (ValueError, KeyError, TypeError) as error:
            return web.json_response({"error": str(error)}, status=400)

    async def health(request):
        return web.json_response(
            {
                "model": args.model_id,
                "protocol": args.protocol,
                "device": args.device,
                **counts,
            }
        )

    app = web.Application(client_max_size=2**20)
    app.router.add_get("/health", health)
    app.router.add_post(
        "/v1/embeddings" if args.protocol == "embedding" else "/v1/chat/completions",
        handle,
    )
    web.run_app(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
