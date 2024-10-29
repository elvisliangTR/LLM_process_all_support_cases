# ClaudeBedrockCaller.py

import boto3
import json
import time
import logging
from functools import wraps
from model_config import MODEL_PARAMS

logger = logging.getLogger(__name__)

def timer(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        start_time = time.time()
        result = func(self, *args, **kwargs)
        end_time = time.time()
        if func.__name__ != '_invoke_claude_model':  # Exclude _invoke_claude_model from logging
            self.logger.info(f"{func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

class ClaudeBedrockCaller:
    def __init__(self, region_name='us-east-1', log_level=logging.INFO, model_params=None):
        self.region_name = region_name
        self.bedrock_endpoint = f'https://bedrock.{region_name}.amazonaws.com'
        self.bedrock_runtime_endpoint = f'https://bedrock-runtime.{region_name}.amazonaws.com'
        self.bedrock = self._create_client('bedrock')
        self.bedrock_runtime = self._create_client('bedrock-runtime')
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        
        # Prevent propagation to the root logger
        self.logger.propagate = False
        
        # Add handler if it does not already exist
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            # Customize the formatter to exclude the timestamp
            formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Set model parameters
        self.model_params = MODEL_PARAMS if model_params is None else model_params

    def _create_client(self, service_name):
        return boto3.client(
            service_name=service_name,
            region_name=self.region_name,
            endpoint_url=self.bedrock_endpoint if service_name == 'bedrock' else self.bedrock_runtime_endpoint
        )

    @timer
    def process_dataframe(self, df, prompt_template):
        self.logger.info(f"Starting to process dataframe with {len(df)} rows")
        processed_results = []
        total_api_time = 0

        for index, row in df.iterrows():
            prompt_data = prompt_template.format(**row)
            start_time = time.time()
            response = self._invoke_claude_model(prompt_data)
            api_time = time.time() - start_time
            total_api_time += api_time
            processed_results.append(response)

        df['processed_result'] = processed_results

        self.logger.info(f"Total API call time: {total_api_time:.2f} seconds")
        self.logger.info(f"Average API call time: {total_api_time/len(df):.2f} seconds")
        return df

    @timer
    def _invoke_claude_model(self, prompt_data):
        body = json.dumps({
            "anthropic_version": self.model_params["anthropic_version"],
            "max_tokens": self.model_params["max_tokens"],
            "temperature": self.model_params["temperature"],
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt_data}],
                }
            ],
        })

        try:
            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_params["model_id"],
                body=body
            )

            result = json.loads(response.get("body").read())
            output_list = result.get("content", [])
            return ' '.join([output["text"] for output in output_list])
        except Exception as e:
            self.logger.error(f"Error invoking Claude model: {str(e)}")
            return None

if __name__ == "__main__":
    pass

logger.info("ClaudeBedrockCaller module imported successfully")