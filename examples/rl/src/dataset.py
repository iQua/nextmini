import re
from datasets import load_dataset

def extract_answer(completion):
    """
    Extracts the numerical answer from the completion.
    GSM8K solutions usually end with #### <number>
    """
    # Look for the last number after ####
    match = re.search(r"####\s*(-?\d+\.?\d*)", completion)
    if match:
        return match.group(1).strip()
    
    # Fallback: sometimes models just output the number at the end
    # But for robust training we enforce the format or check the very last number
    # For this minimal example, let's stick to the #### format or simple last number if strict
    # We'll try to find the last number in the text if #### is missing, 
    # but standard GSM8K training usually trains the model to output ####.
    
    matches = re.findall(r"(-?\d+\.?\d*)", completion)
    if matches:
        return matches[-1]
    return None

def is_correct(completion, ground_truth):
    pred = extract_answer(completion)
    gt = extract_answer(ground_truth)
    
    if pred is None or gt is None:
        return False
    
    try:
        # Compare as floats to handle 1.0 vs 1
        return abs(float(pred) - float(gt)) < 1e-6
    except ValueError:
        return pred == gt

class GSM8KLoader:
    def __init__(self, split="train"):
        self.dataset = load_dataset("gsm8k", "main", split=split)
    
    def __len__(self):
        return len(self.dataset)
    
    def __getitem__(self, idx):
        item = self.dataset[idx]
        return {
            "question": item["question"],
            "answer": item["answer"]
        }
    
    def get_batch(self, batch_size):
        # Random sampling or sequential
        import random
        indices = random.sample(range(len(self)), batch_size)
        return [self[i] for i in indices]
