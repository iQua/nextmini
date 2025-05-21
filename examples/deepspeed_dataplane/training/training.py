from argparse import ArgumentParser
from pytorch_lightning import Trainer
from torch.utils.data import DataLoader
import pytorch_lightning as pl
import torch
from itertools import chain
from datasets import load_dataset
from deepspeed.ops.adam import FusedAdam
from time import time
from transformers import (
    OPTForCausalLM,
    AutoTokenizer,
    default_data_collator,
)
from typing import Optional
torch.cuda.manual_seed(328425)

# 350m 1.3b 2.7b 6.7b
def getDataset():
    raw_datasets = load_dataset("wikitext", "wikitext-2-v1")
    tokenizer = AutoTokenizer.from_pretrained("facebook/opt-350m")
    column_names = raw_datasets["train"].column_names
    text_column_name = "text" if "text" in column_names else column_names[0]

    def tokenize_function(examples):
        return tokenizer(examples[text_column_name])

    tokenized_datasets = raw_datasets.map(
        tokenize_function,
        batched=True,
        num_proc=1,
        remove_columns=column_names,
        load_from_cache_file=False,
        desc="Running tokenizer on dataset",
    )

    def group_texts(examples):
        # Concatenate all texts.
        concatenated_examples = {
            k: list(chain(*examples[k])) for k in examples.keys()}
        total_length = len(concatenated_examples[list(examples.keys())[0]])

        if total_length >= 1024:
            total_length = (total_length // 1024) * 1024
        # Split by chunks of max_len.
        result = {
            k: [t[i: i + 1024]
                for i in range(0, total_length, 1024)]
            for k, t in concatenated_examples.items()
        }
        result["labels"] = result["input_ids"].copy()
        return result

    lm_datasets = tokenized_datasets.map(
        group_texts,
        batched=True,
        num_proc=1,
        load_from_cache_file=False,
        desc=f"Grouping texts in chunks of {1024}",
    )

    return lm_datasets["train"]


class OPT(pl.LightningModule):
    def __init__(self,
                 optmodel,
                 weight_decay=0.1,
                 betas=(0.9, 0.95),
                 learning_rate=3e-4,
                 ):
        super().__init__()
        self.model = optmodel
        self.weight_decay = weight_decay
        self.betas = betas
        self.learning_rate = learning_rate

    def configure_optimizers(self):
        no_decay = ["bias", "LayerNorm.weight"]
        params_decay = [p for n, p in self.named_parameters(
        ) if not any(nd in n for nd in no_decay)]
        params_nodecay = [p for n, p in self.named_parameters() if any(
            nd in n for nd in no_decay)]
        optim_groups = [
            {"params": params_decay, "weight_decay": self.weight_decay},
            {"params": params_nodecay, "weight_decay": 0.0},
        ]
        return FusedAdam(optim_groups, lr=self.learning_rate, betas=self.betas)

    def forward(self, input_ids: torch.LongTensor = None,
                attention_mask: Optional[torch.Tensor] = None,
                labels: Optional[torch.LongTensor] = None,):
        output = self.model.forward(input_ids=input_ids,
                                    attention_mask=attention_mask, labels=labels)
        return output

    def training_step(self, batch, batch_idx):
        input_ids = batch["input_ids"]
        attention_mask = batch["attention_mask"]
        labels = batch["labels"]
        output = self(input_ids=input_ids,
                      attention_mask=attention_mask, labels=labels)
        loss = output.loss
        return loss


if __name__ == '__main__':

    # parser = ArgumentParser()
    # parser.add_argument('--learning_rate', default=6e-4, type=float)
    # parser.add_argument('--block_size', default=128, type=int)
    # parser.add_argument('--batch_size', default=1, type=int)
    # parser.add_argument('--num_workers', default=0, type=int)
    # args = parser.parse_args()

    # one line of poem is roughly 50 characters

    train_dataset = getDataset()
    train_loader = DataLoader(
        train_dataset, collate_fn=default_data_collator,
        batch_size=2, num_workers=8
    )

    model = OPTForCausalLM.from_pretrained("facebook/opt-2.7b")
    # with init_meta_context():
    model = OPT(model)

    trainer = Trainer(
        accelerator="gpu",
        strategy="deepspeed_stage_3",
        devices=1,
        num_nodes=2,
        max_epochs=1,
        gradient_clip_val=1.0,
        precision=16
    )
    time_start = time()
    trainer.fit(model, train_loader)
    training_duration = time() - time_start
    print(training_duration)