import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from src.dataset import GSM8KLoader, is_correct
from src import config
from tqdm import tqdm


def evaluate_model(model_name_or_path, split="test", num_samples=50, device=None):
    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"

    print(
        f"Evaluating model: {model_name_or_path} on split='{split}', num_samples={num_samples}"
    )

    tokenizer = AutoTokenizer.from_pretrained(
        model_name_or_path, trust_remote_code=True
    )
    model = AutoModelForCausalLM.from_pretrained(
        model_name_or_path,
        trust_remote_code=True,
    ).to(device)
    model.eval()

    dataset = GSM8KLoader(split)
    n = min(num_samples, len(dataset))

    correct = 0
    results = []

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
    print(f"Accuracy: {acc:.4f} ({correct}/{n})")

    del model
    torch.cuda.empty_cache()

    return results, acc


def main():
    num_samples = 500
    split = "test"
    device = "cuda" if torch.cuda.is_available() else "cpu"

    print("=== Evaluating baseline model ===")
    base_results, base_acc = evaluate_model(
        config.MODEL_NAME,
        split=split,
        num_samples=num_samples,
        device=device,
    )

    print("\n=== Evaluating RL-trained model ===")
    trained_model_path = "output/qwen-gsm8k-rl"
    rl_results, rl_acc = evaluate_model(
        trained_model_path,
        split=split,
        num_samples=num_samples,
        device=device,
    )

    print("\n=== Final comparison ===")
    print(f"Baseline accuracy: {base_acc:.4f}")
    print(f"RL-trained accuracy: {rl_acc:.4f}")
    print(f"Absolute improvement: {rl_acc - base_acc:.4f}")

    print("\nExamples where RL model fixed baseline errors:")
    shown = 0
    for i in range(len(base_results)):
        if not base_results[i]["is_correct"] and rl_results[i]["is_correct"]:
            print("-" * 80)
            print("Question:", base_results[i]["question"])
            print("Ground truth:", base_results[i]["ground_truth"])
            print("Baseline completion:", base_results[i]["completion"])
            print("RL-trained completion:", rl_results[i]["completion"])
            shown += 1
            if shown >= 5:
                break
    if shown == 0:
        print("No such examples in this subset.")


if __name__ == "__main__":
    main()
