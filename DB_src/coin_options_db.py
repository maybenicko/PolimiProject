import requests
import json
import re


class MemecoinGenerator:
    def __init__(self, post_text, subreddit_name):
        with open('creds.json', 'r') as file:
            data = json.load(file)
        self.openai_api_key = data['openai_api']
        self.pinata_api_key = data['pinata_api']
        self.pinata_secret_key = data['pinata_secret']
        self.post = post_text
        self.sub = subreddit_name

    def process_reddit_post(self):
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {self.openai_api_key}", "Content-Type": "application/json"}

        prompt = f"""
        Convert the following Reddit post into a Memecoin concept.
        **DO NOT RETURN ANYTHING OTHER THAN THE JSON OBJECT IN THE REQUIRED FORMAT.**

        **Processing Steps:**
        1️⃣ **Categorize** the post into the category you feel is the most appropriate
        2️⃣ **Analyze the sentiment** of the post:
           - **Positive:** Excited, hyped, mooning
           - **Neutral:** Informational, general update
           - **Negative:** Rekt, panic, cope
        3️⃣ **Based on category and sentiment, generate:**
           - A **fun, meme-worthy name**.
           - A **symbol** (max 5 characters).
           - A **humorous description** that aligns with sentiment.
           - A **DALL·E image prompt** that visually represents the Memecoin (with PEPE as a protagonist).

        **Reddit Post Context (For Processing Only, NOT in Output):**
        **Subreddit:** r/{self.sub}
        **Post Content:** "{self.post}"

        **Required JSON Output Format (No Extra Words, Category & Sentiment Used but NOT in Output):**
        ```json
        {{
          "name": "[Generated Memecoin Name]",
          "symbol": "$[TICKER]",
          "description": "[Generated Meme-Worthy Description]",
          "image_prompt": "[Generated Image Prompt for DALL·E]"
        }}
        ```
        """

        payload = {
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": prompt}],
            "max_tokens": 300
        }

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            raise ValueError(f"API Error: {response.status_code} - {response.text}")

        try:
            raw_content = response.json()["choices"][0]["message"]["content"]
            clean_json = re.sub(r"```json\n|\n```", "", raw_content).strip()
            return json.loads(clean_json)
        except json.JSONDecodeError as e:
            print("JSON Decode Error:", e)
            print("Raw Content Received:", raw_content)
            raise ValueError("Invalid JSON response from OpenAI API")

    def generate_meme_image(self, image_prompt):
        url = "https://api.openai.com/v1/images/generations"
        headers = {"Authorization": f"Bearer {self.openai_api_key}", "Content-Type": "application/json"}

        payload = {
            "model": "dall-e-3",
            "prompt": image_prompt,
            "n": 1,
            "size": "1024x1024"
        }

        response = requests.post(url, json=payload, headers=headers)
        return response.json()["data"][0]["url"]

    def upload_to_ipfs(self, image_url):
        image_data = requests.get(image_url).content

        pinata_url = "https://api.pinata.cloud/pinning/pinFileToIPFS"
        headers = {
            "pinata_api_key": self.pinata_api_key,
            "pinata_secret_api_key": self.pinata_secret_key
        }

        files = {"file": ("memecoin_image.png", image_data)}
        response = requests.post(pinata_url, headers=headers, files=files)

        if response.status_code == 200:
            ipfs_hash = response.json()["IpfsHash"]
            return f"https://ipfs.io/ipfs/{ipfs_hash}"
        else:
            raise Exception(f"IPFS Upload Failed: {response.text}")

    def gen_metadata(self):
        print('[ PROCESSING POST... ]')
        memecoin_data = self.process_reddit_post()
        image_prompt = memecoin_data["image_prompt"]
        print('[ GENERATING IMAGE... ]')
        meme_image_url = self.generate_meme_image(image_prompt)
        print('[ UPLOADING IPFS... ]')
        ipfs_image_url = self.upload_to_ipfs(meme_image_url)

        data = {
            "name": memecoin_data["name"],
            "symbol": memecoin_data["symbol"],
            "description": memecoin_data["description"],
            "image": ipfs_image_url
        }
        return data

