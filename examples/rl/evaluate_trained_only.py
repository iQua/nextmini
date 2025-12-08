import torch
import argparse
import os
from transformers import AutoModelForCausalLM, AutoTokenizer
from src.dataset import GSM8KLoader, is_correct
from src import config
from tqdm import tqdm


def evaluate_model(model_name_or_path, split="test", num_samples=500, device=None):
    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"

    print(
        f"Evaluating model: {model_name_or_path} on split='{split}', num_samples={num_samples}"
    )
    print(f"Device: {device}")

    try:
        tokenizer = AutoTokenizer.from_pretrained(
            model_name_or_path, trust_remote_code=True
        )
        model = AutoModelForCausalLM.from_pretrained(
            model_name_or_path,
            trust_remote_code=True,
            device_map=device if device == "cuda" else None,
        )
        if device != "cuda":
            model = model.to(device)

        model.eval()
    except Exception as e:
        print(f"Error loading model from {model_name_or_path}: {e}")
        return [], 0.0

    dataset = GSM8KLoader(split)
    n = min(num_samples, len(dataset))

    correct = 0
    results = []

    print(f"Starting evaluation on {n} samples...")
    for idx in tqdm(range(n)):
        item = dataset[idx]
        prompt = item["question"]
        gt = item["answer"]

        inputs = tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=config.MAX_SEQ_LEN,
        ).to(device)

        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=config.GENERATION_LEN,
                do_sample=False,
                temperature=0.0,
                pad_token_id=tokenizer.eos_token_id,
            )

        full_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
        if full_text.startswith(prompt):
            completion = full_text[len(prompt) :]
        else:
            completion = full_text

        ok = is_correct(completion, gt)
        if ok:
            correct += 1

        results.append(
            {
                "question": prompt,
                "ground_truth": gt,
                "completion": completion,
                "is_correct": ok,
            }
        )

    acc = correct / n if n > 0 else 0.0
    print(f"\nResults for {model_name_or_path}:")
    print(f"Accuracy: {acc:.4f} ({correct}/{n})")

    return results, acc


def main():
    parser = argparse.ArgumentParser(description="Evaluate RL-trained model on GSM8K")
    parser.add_argument(
        "--model_path",
        type=str,
        default="output/qwen-gsm8k-rl",
        help="Path to the trained model directory",
    )
    parser.add_argument(
        "--samples", type=int, default=500, help="Number of samples to evaluate"
    )

    args = parser.parse_args()

    # Check if path exists, if not try looking in project root
    if not os.path.exists(args.model_path):
        # Check if we are in examples/rl and model is in ../../output
        root_output = os.path.join("..", "..", args.model_path)
        if os.path.exists(root_output):
            print(
                f"Model not found at {args.model_path}, found at {root_output}. Using that."
            )
            args.model_path = root_output
        else:
            print(f"Warning: Model path {args.model_path} does not exist.")

    evaluate_model(args.model_path, split="test", num_samples=args.samples)


if __name__ == "__main__":
    main()
