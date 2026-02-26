import os, sys

from dotenv import load_dotenv

MODULE_DIR_PATH = os.path.dirname(os.path.dirname(__file__))
if __name__ == "__main__":
    sys.path.append(os.path.dirname(MODULE_DIR_PATH))

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(MODULE_DIR_PATH)), ".env"))

from gpt import GPTAWS

api = GPTAWS(
    AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID"),
    AWS_ACCESS_KEY_SECRET=os.getenv("AWS_ACCESS_KEY_SECRET"),
    AWS_REGION_NAME=os.getenv("AWS_REGION_NAME")
)

answer = api.return_generation(model_name="nova-lite", user_prompt="test")
print(answer)

for chunk in api.yield_generation(model_name="nova-lite", user_prompt="test"):
    print(chunk)
    answer = chunk
