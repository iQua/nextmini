deepspeed --num_gpus 1 \
    --num_nodes 2 \
    --hostfile ./hostfile_two \
    --master_addr 10.0.0.1 \
    --master_port 9901 \
    run_clm_lora_pp.py \
    --deepspeed ./dsconfig.json \
    --fp16 \
    --model_name_or_path facebook/opt-2.7b\
    --use_fast_tokenizer False\
    --per_device_train_batch_size 1 \
    --do_train \
    --per_device_eval_batch_size 1 \
    --do_eval \
    --dataset_name wikitext \
    --dataset_config_name wikitext-2-raw-v1 \
    --max_train_samples 100 \
    --max_eval_samples 100 \
    --overwrite_output_dir true\
    --overwrite_cache true \
    --output_dir ./finetune/test-clm\
    --logging_dir './logs' \
    --logging_steps 1  

