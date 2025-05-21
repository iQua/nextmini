python run_clms.py \
    --fp16 \
    --model_name_or_path facebook/opt-125m\
    --use_fast_tokenizer False\
    --per_device_train_batch_size 1 \
    --do_train \
    --per_device_eval_batch_size 1 \
    --do_eval \
    --dataset_name wikitext \
    --dataset_config_name wikitext-2-raw-v1 \
    --max_train_samples 100 \
    --max_eval_samples 100 \
    --num_train_epochs 1 \
    --overwrite_output_dir true\
    --overwrite_cache true \
    --output_dir ./finetune/test-clm\
    --logging_dir './logs' \
    --logging_steps 1  

