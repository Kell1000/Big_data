"""
Image Processor — Image classification and feature extraction using PyTorch.
Supports GPU acceleration when available.

Usage:
    from src.image_processor import ImageClassifier
    classifier = ImageClassifier()
    results = classifier.classify_batch(image_paths)
"""
import logging
import os
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from torchvision import models, transforms
from PIL import Image

logger = logging.getLogger(__name__)

# ── Device configuration ────────────────────────────────────────────────────
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info("PyTorch device: %s", DEVICE)
if torch.cuda.is_available():
    logger.info("GPU: %s", torch.cuda.get_device_name(0))


# ── ImageNet labels → meta-categories ───────────────────────────────────────
META_CATEGORIES = {
    "animals": {
        "dog", "cat", "bird", "fish", "snake", "turtle", "rabbit", "hamster",
        "bear", "wolf", "fox", "deer", "horse", "elephant", "lion", "tiger",
        "monkey", "gorilla", "penguin", "eagle", "owl", "shark", "whale",
        "dolphin", "frog", "lizard", "spider", "bee", "butterfly", "ant",
    },
    "food": {
        "pizza", "burger", "sandwich", "salad", "soup", "cake", "bread",
        "pasta", "sushi", "fruit", "ice cream", "chocolate", "coffee",
        "wine", "beer", "steak", "chicken", "rice", "cheese", "egg",
    },
    "vehicles": {
        "car", "truck", "bus", "motorcycle", "bicycle", "airplane", "boat",
        "train", "helicopter", "rocket", "taxi",
    },
    "electronics": {
        "laptop", "computer", "phone", "television", "monitor", "keyboard",
        "mouse", "camera", "headphone", "speaker", "tablet", "printer",
    },
    "nature": {
        "mountain", "ocean", "lake", "river", "forest", "desert", "beach",
        "volcano", "waterfall", "sunset", "sunrise", "cloud", "snow",
    },
    "people": {
        "person", "man", "woman", "child", "baby", "face", "hand", "group",
    },
    "buildings": {
        "house", "building", "church", "castle", "bridge", "tower", "palace",
        "skyscraper", "mosque", "temple", "school", "hospital",
    },
}


def label_to_meta_category(label: str) -> str:
    """Map an ImageNet label to a meta-category."""
    label_lower = label.lower()
    for category, keywords in META_CATEGORIES.items():
        for kw in keywords:
            if kw in label_lower:
                return category
    return "other"


# ── Image preprocessing ────────────────────────────────────────────────────
PREPROCESS = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(
        mean=[0.485, 0.456, 0.406],
        std=[0.229, 0.224, 0.225],
    ),
])


class ImageClassifier:
    """
    Image classifier using ResNet50 pre-trained on ImageNet.
    Supports GPU acceleration.
    """

    def __init__(self, device=None):
        self.device = device or DEVICE
        logger.info("Loading ResNet50 model on %s...", self.device)

        # Load model
        self.model = models.resnet50(weights=models.ResNet50_Weights.IMAGENET1K_V2)
        self.model = self.model.to(self.device)
        self.model.eval()

        # Feature extractor (remove last FC layer)
        self.feature_extractor = nn.Sequential(
            *list(self.model.children())[:-1]
        ).to(self.device)
        self.feature_extractor.eval()

        # Load ImageNet labels
        self.labels = models.ResNet50_Weights.IMAGENET1K_V2.meta["categories"]

        logger.info("ResNet50 loaded (%d classes)", len(self.labels))

    def _load_image(self, image_path: str) -> torch.Tensor | None:
        """Load and preprocess a single image."""
        try:
            img = Image.open(image_path).convert("RGB")
            return PREPROCESS(img)
        except Exception as e:
            logger.warning("Failed to load image %s: %s", image_path, e)
            return None

    def classify_single(self, image_path: str, top_k: int = 5) -> dict:
        """
        Classify a single image.
        Returns: {label, confidence, top_k_labels, meta_category}
        """
        tensor = self._load_image(image_path)
        if tensor is None:
            return {"label": "error", "confidence": 0.0, "meta_category": "error"}

        with torch.no_grad():
            input_batch = tensor.unsqueeze(0).to(self.device)
            output = self.model(input_batch)
            probabilities = torch.nn.functional.softmax(output[0], dim=0)

        top_probs, top_indices = torch.topk(probabilities, top_k)
        top_labels = [(self.labels[idx], prob.item()) for idx, prob in zip(top_indices, top_probs)]

        best_label, best_conf = top_labels[0]
        return {
            "image_path": image_path,
            "label": best_label,
            "confidence": best_conf,
            "top_k": top_labels,
            "meta_category": label_to_meta_category(best_label),
        }

    def classify_batch(self, image_paths: list[str], batch_size: int = 32) -> list[dict]:
        """
        Classify multiple images in batches (GPU accelerated).
        Returns list of classification results.
        """
        from tqdm import tqdm

        results = []
        logger.info("Classifying %d images (batch_size=%d, device=%s)...",
                     len(image_paths), batch_size, self.device)

        for i in tqdm(range(0, len(image_paths), batch_size), desc="Classifying"):
            batch_paths = image_paths[i:i + batch_size]
            batch_tensors = []
            valid_paths = []

            for path in batch_paths:
                tensor = self._load_image(path)
                if tensor is not None:
                    batch_tensors.append(tensor)
                    valid_paths.append(path)

            if not batch_tensors:
                continue

            # Stack into batch and run on GPU
            batch = torch.stack(batch_tensors).to(self.device)

            with torch.no_grad():
                outputs = self.model(batch)
                probs = torch.nn.functional.softmax(outputs, dim=1)

            for j, path in enumerate(valid_paths):
                top_probs, top_indices = torch.topk(probs[j], 5)
                top_labels = [(self.labels[idx], prob.item())
                              for idx, prob in zip(top_indices, top_probs)]
                best_label, best_conf = top_labels[0]

                results.append({
                    "image_path": path,
                    "label": best_label,
                    "confidence": best_conf,
                    "top_k": top_labels,
                    "meta_category": label_to_meta_category(best_label),
                })

        logger.info("Classified %d images", len(results))
        return results

    def extract_features(self, image_paths: list[str], batch_size: int = 32) -> np.ndarray:
        """
        Extract deep features (2048-dim vectors) from images using ResNet50.
        Suitable for use as ML features.
        Returns numpy array of shape (n_images, 2048).
        """
        from tqdm import tqdm

        all_features = []
        logger.info("Extracting features from %d images...", len(image_paths))

        for i in tqdm(range(0, len(image_paths), batch_size), desc="Extracting features"):
            batch_paths = image_paths[i:i + batch_size]
            batch_tensors = []

            for path in batch_paths:
                tensor = self._load_image(path)
                if tensor is not None:
                    batch_tensors.append(tensor)

            if not batch_tensors:
                continue

            batch = torch.stack(batch_tensors).to(self.device)

            with torch.no_grad():
                features = self.feature_extractor(batch)
                features = features.squeeze(-1).squeeze(-1)  # (batch, 2048)
                all_features.append(features.cpu().numpy())

        if all_features:
            return np.concatenate(all_features, axis=0)
        return np.array([])


# ── Image statistics ────────────────────────────────────────────────────────
def get_image_stats(image_path: str) -> dict:
    """
    Get basic image statistics (size, aspect ratio, dominant color, brightness).
    """
    try:
        img = Image.open(image_path).convert("RGB")
        width, height = img.size
        pixels = np.array(img)

        return {
            "width": width,
            "height": height,
            "aspect_ratio": round(width / height, 2),
            "avg_brightness": float(np.mean(pixels)),
            "avg_r": float(np.mean(pixels[:, :, 0])),
            "avg_g": float(np.mean(pixels[:, :, 1])),
            "avg_b": float(np.mean(pixels[:, :, 2])),
            "file_size_kb": os.path.getsize(image_path) // 1024,
        }
    except Exception as e:
        logger.warning("Failed to get stats for %s: %s", image_path, e)
        return {}
