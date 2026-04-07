"""
Simple wrapper for calling the xAI (Grok) API via the official xai-sdk.
Docs: https://docs.x.ai/api
"""

from xai_sdk.sync.client import Client
from xai_sdk.chat import user  # helper that formats a user-role message

from credentials import grok_token


def ask_grok(prompt: str, model: str = "grok-4.20-reasoning-latest") -> str:
    """
    Send a prompt to Grok and return the response text.

    Args:
        prompt: The question or instruction to send.
        model:  xAI model name. Defaults to the latest Grok 4.20 reasoning model.

    Returns:
        The model's response as a string.
    """
    # Client connects via gRPC; we pass the key explicitly instead of via env var
    with Client(api_key=grok_token) as client:
        chat = client.chat.create(model=model)
        chat.append(user(prompt))          # add the user's message to the conversation
        response = chat.sample()           # call the API and get one response
    return response.content


if __name__ == "__main__":
    answer = ask_grok("What is the meaning of life, the universe, and everything?")
    print(answer)
