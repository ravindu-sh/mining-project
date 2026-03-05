from openai import OpenAI
import json
from typing import List

from openai.types.chat import ChatCompletionMessageParam
from openai.types.chat.completion_create_params import ResponseFormat


def get_project_names(text: str) -> List[str]:
    client = OpenAI(base_url="http://localhost:1234/v1", api_key="lm-studio")

    messages: List[ChatCompletionMessageParam] = [
        {
            "role": "system",
            "content": "You are an information extraction agent to extract mine names and mining related project names from news articles.",
        },
        {"role": "user", "content": text},
    ]

    schema: ResponseFormat = {
        "type": "json_schema",
        "json_schema": {
            "name": "mining_project_or_mine_names_extraction",
            "schema": {
                "type": "object",
                "properties": {
                    "mining_project_or_mine_names": {
                        "type": "array",
                        "items": {"type": "string"},
                    }
                },
                "required": ["mining_project_or_mine_names"],
                "additionalProperties": False,
            },
        },
    }

    # Get response from AI
    response = client.chat.completions.create(
        model="your-model",
        messages=messages,
        response_format=schema,
    )

    # Parse and display the results
    if len(response.choices) < 0 or response.choices[0].message.content is None:
        return []
    results = json.loads(response.choices[0].message.content)
    if results["mining_project_or_mine_names"]:
        return results["mining_project_or_mine_names"]
    else:
        return []
