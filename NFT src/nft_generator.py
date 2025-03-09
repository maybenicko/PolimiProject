import os
import random
from PIL import Image
import json


base_path = os.path.join(os.path.dirname(__file__), "BART LAYERS")
output_images_path = os.path.join(os.path.dirname(__file__), "output_images")
output_metadata_path = os.path.join(os.path.dirname(__file__), "output_metadata")

folders = ["0_background", "1_outline", "2_head", "3_mouth", "4_body", "5_eyes", "6_hat", "7_accessory", "8_ears"]


rarity_config = {
    "0_background": {"blue.png": 17, "green.png": 17, "orange.png": 17, "purple.png": 17, "red.png": 17,
                     "yellow.png": 15},
    "1_outline": {"outline.png": 100},
    "2_head": {"alien.png": 8, "skeleton.png": 8, "yellow.png": 76, "zombie.png": 8},
    "3_mouth": {"amazed.png": 15, "happy.png": 35, "sad.png": 30, "high.png": 20},
    "4_body": {"armor.png": 2.8, "beige smoking.png": 4.45, "blue jacket.png": 4, "blue smoking.png": 4.45,
               "blue.png": 5, "bordeaux.png": 5, "brown jacket.png": 4, "brown smoking.png": 4.45, "brown.png": 5,
               "cop vest.png": 2.8, "green smoking.png": 4.45, "green.png": 5, "grey.png": 5, "military vest.png": 2.8,
               "multi.png": 5, "orange jacket.png": 4, "orange.png": 5, "pink.png": 5, "pirate vest.png": 2.8,
               "purple jacket.png": 4, "purple.png": 5, "red.png": 5, "yellow.png": 5},
    "5_eyes": {"3d glasses.png": 6, "blue glasses.png": 7, "ded.png": 11, "high af.png": 8, "normal.png": 25,
               "red glasses.png": 7, "solana vipers.png": 6, "thug glasses.png": 7, "thug purple glasses.png": 7,
               "vipers.png": 6, "yellow.png": 10},
    "6_hat": {"arrow.png": 3.84, "black top hat.png": 6.8, "cop hat.png": 6.2, "cowboy brown.png": 6.8,
              "cowboy grey.png": 6.8, "firefighter hat.png": 6.2, "goku.png": 3.84, "green beret.png": 6.8,
              "green top hat.png": 6.8, "horns grey.png": 6.2, "none.png": 8.12, "pirate bandana.png": 6.2,
              "red beret.png": 6.8, "santa.png": 6.2, "sombrero.png": 6.2, "horns purple.png": 6.2},
    "7_accessory": {"none.png": 60, "tongue.png": 17, "lollipop.png": 8, "cigarette.png": 15},
    "8_ears": {"none.png": 70, "golden earrings.png": 10, "silver earrings.png": 20}
}

dependency_rules = {
    ("3_mouth", "amazed.png"): {"7_accessory": "none.png"}
}

compatibility_rules = {
    # ("hat", "trait1.png"): {"body": ["trait1.png", "trait2.png"]}
}

restriction_rules = {
    ("2_head", "zombie.png"): {"4_body": ["green smoking.png", "green.png"]},
    ("2_head", "zombie.png"): {"6_hat": ["green beret.png", "green top hat.png"]},
    ("2_head", "skeleton.png"): {"6_hat": ["horns grey.png"]},
    ("2_head", "alien.png"): {"5_eyes": ["blue glasses.png"]}
}


def select_trait(trait_dict):
    traits = list(trait_dict.keys())
    probabilities = list(trait_dict.values())
    return random.choices(traits, probabilities)[0]


def generate_nft():
    nft_traits = {}
    for folder in folders:
        trait = select_trait(rarity_config[folder])
        nft_traits[folder] = trait

    for (dep_folder, dep_trait), required_traits in dependency_rules.items():
        if nft_traits.get(dep_folder) == dep_trait:
            for req_folder, req_trait in required_traits.items():
                nft_traits[req_folder] = req_trait

    for (comp_folder, comp_trait), allowed_traits in compatibility_rules.items():
        if nft_traits.get(comp_folder) == comp_trait:
            for target_folder, valid_traits in allowed_traits.items():
                if nft_traits[target_folder] not in valid_traits:
                    nft_traits[target_folder] = random.choice(valid_traits)

    for (rest_folder, rest_trait), restricted_traits in restriction_rules.items():
        if nft_traits.get(rest_folder) == rest_trait:
            for target_folder, invalid_traits in restricted_traits.items():
                if nft_traits[target_folder] in invalid_traits:
                    valid_options = [t for t in rarity_config[target_folder].keys() if t not in invalid_traits]
                    if valid_options:
                        nft_traits[target_folder] = random.choice(valid_options)

    return nft_traits


def create_nft_image(nft_traits, output_path="generated_nft.png"):
    layers = []
    for folder, trait in nft_traits.items():
        trait_path = os.path.join(base_path, folder, trait)
        if os.path.exists(trait_path):
            layers.append(Image.open(trait_path).convert("RGBA"))

    if layers:
        base_image = layers[0]
        for layer in layers[1:]:
            base_image.paste(layer, (0, 0), layer)
        base_image.save(output_path)
    else:
        print("No valid layers found to generate image.")


def create_metadata(nft_traits, index):
    # add count for number of attributes
    image_index = str(index+1).zfill(4)
    image_name = f"Bart #{image_index}"
    attributes = []

    for folder, trait in nft_traits.items():
        trait_name = folder.split('_', 1)[1] if '_' in folder else folder
        trait_value = trait.rsplit('.', 1)[0]
        if trait_name != "outline":
            attributes.append({"trait_type": trait_name, "value": trait_value})

    metadata = {
        "name": image_name,
        "description": "One of 20 Bart NFTs.",
        "image": "ipfs://",
        "attributes": attributes
    }

    metadata_save_path = os.path.join(output_metadata_path, f"{image_index}.json")
    with open(metadata_save_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)


def generate_nfts(count=10):
    for i in range(count):
        nft_traits = generate_nft()
        image_path = os.path.join(output_images_path, f"BART #{str(i+1).zfill(4)}.png")
        create_nft_image(nft_traits, image_path)
        create_metadata(nft_traits, i)
        print(f"[ Generated NFT {i} ]")


generate_nfts(1111)
