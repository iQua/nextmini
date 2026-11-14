import transformers
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer


def load_tokenizer(model_name_or_path):
    tokenizer = AutoTokenizer.from_pretrained(model_name_or_path, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    return tokenizer


def _model_class_for(model_name_or_path):
    cfg = AutoConfig.from_pretrained(model_name_or_path, trust_remote_code=True)
    model_type = str(getattr(cfg, "model_type", "") or "")
    architectures = set(getattr(cfg, "architectures", []) or [])

    if model_type == "qwen3_5" or "Qwen3_5ForConditionalGeneration" in architectures:
        auto_cls = getattr(transformers, "AutoModelForImageTextToText", None)
        if auto_cls is not None:
            return auto_cls

        qwen_cls = getattr(transformers, "Qwen3_5ForConditionalGeneration", None)
        if qwen_cls is not None:
            return qwen_cls

        raise RuntimeError(
            "Qwen3.5 requires a Transformers build with Qwen3_5ForConditionalGeneration "
            "or AutoModelForImageTextToText support."
        )

    return AutoModelForCausalLM


def load_policy_model(model_name_or_path, *, dtype):
    model_cls = _model_class_for(model_name_or_path)
    return model_cls.from_pretrained(
        model_name_or_path,
        dtype=dtype,
        trust_remote_code=True,
    )
