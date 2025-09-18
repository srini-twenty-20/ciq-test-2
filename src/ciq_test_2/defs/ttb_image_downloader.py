"""
TTB Image Download Utilities.

This module handles downloading label images and signatures from TTB certificate pages.
"""
import time
import hashlib
import tempfile
from pathlib import Path
from typing import Dict, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class TTBImageDownloader:
    """Download images from TTB certificate pages with error handling and rate limiting."""

    def __init__(self, base_url: str = "https://ttbonline.gov"):
        self.base_url = base_url
        self.session = self._create_session()
        self.download_errors = []

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy and proper headers."""
        session = requests.Session()

        # Add retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set headers to mimic a real browser
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        return session

    def download_image(self, image_url: str, ttb_id: str, image_type: str = "unknown") -> Dict[str, Any]:
        """
        Download a single image from TTB.

        Args:
            image_url: Relative or absolute URL to the image
            ttb_id: TTB ID for organization/logging
            image_type: Type of image (front_label, back_label, signature)

        Returns:
            Dict with download results and metadata
        """
        result = {
            'ttb_id': ttb_id,
            'image_type': image_type,
            'original_url': image_url,
            'full_url': None,
            'download_success': False,
            'http_status': None,
            'content_type': None,
            'file_size_bytes': None,
            'file_content': None,
            'file_hash': None,
            'error_message': None,
            'download_timestamp': time.time()
        }

        try:
            # Build full URL
            if image_url.startswith('/'):
                full_url = urljoin(self.base_url, image_url)
            else:
                full_url = image_url

            result['full_url'] = full_url

            # Rate limiting - be respectful
            time.sleep(0.5)

            # Make the request
            response = self.session.get(full_url, timeout=30, verify=False)
            result['http_status'] = response.status_code
            result['content_type'] = response.headers.get('content-type', 'unknown')

            if response.status_code == 200:
                # Success! Get the content
                content = response.content
                result['file_content'] = content
                result['file_size_bytes'] = len(content)
                result['file_hash'] = hashlib.sha256(content).hexdigest()
                result['download_success'] = True

                # Validate it's actually an image
                if not self._is_valid_image_content(content, result['content_type']):
                    result['download_success'] = False
                    result['error_message'] = f"Downloaded content doesn't appear to be a valid image. Content-Type: {result['content_type']}, Size: {len(content)} bytes"

            else:
                # HTTP error
                result['error_message'] = f"HTTP {response.status_code}: {response.reason}"
                if response.status_code == 403:
                    result['error_message'] += " (Forbidden - may require authentication)"
                elif response.status_code == 404:
                    result['error_message'] += " (Not Found - image may not exist)"
                elif response.status_code == 429:
                    result['error_message'] += " (Rate Limited - too many requests)"

        except requests.exceptions.Timeout:
            result['error_message'] = "Request timeout (30s)"
        except requests.exceptions.ConnectionError:
            result['error_message'] = "Connection error - unable to reach TTB server"
        except requests.exceptions.RequestException as e:
            result['error_message'] = f"Request error: {str(e)}"
        except Exception as e:
            result['error_message'] = f"Unexpected error: {str(e)}"

        # Log errors
        if not result['download_success']:
            self.download_errors.append(result)

        return result

    def _is_valid_image_content(self, content: bytes, content_type: str) -> bool:
        """Validate that downloaded content is actually an image."""
        if len(content) < 100:  # Too small to be a real image
            return False

        # Check content type
        if content_type and 'image/' in content_type.lower():
            return True

        # Check magic bytes for common image formats
        if content.startswith(b'\xFF\xD8\xFF'):  # JPEG
            return True
        elif content.startswith(b'\x89PNG\r\n\x1a\n'):  # PNG
            return True
        elif content.startswith(b'GIF87a') or content.startswith(b'GIF89a'):  # GIF
            return True
        elif content.startswith(b'RIFF') and b'WEBP' in content[:20]:  # WebP
            return True

        return False

    def download_certificate_images(self, image_list: list, ttb_id: str) -> Dict[str, Any]:
        """
        Download all images from a certificate's image list.

        Args:
            image_list: List of image dicts from certificate parsing
            ttb_id: TTB ID for organization

        Returns:
            Dict with overall download results
        """
        results = {
            'ttb_id': ttb_id,
            'total_images': len(image_list),
            'successful_downloads': 0,
            'failed_downloads': 0,
            'download_results': [],
            'processing_time': 0
        }

        start_time = time.time()

        for i, image_info in enumerate(image_list):
            image_url = image_info.get('original_url')
            image_type = image_info.get('type', f'image_{i+1}')

            if not image_url:
                continue

            # Download the image
            download_result = self.download_image(image_url, ttb_id, image_type)
            results['download_results'].append(download_result)

            if download_result['download_success']:
                results['successful_downloads'] += 1
            else:
                results['failed_downloads'] += 1

        results['processing_time'] = time.time() - start_time
        return results

    def save_image_to_file(self, download_result: Dict[str, Any], output_path: str) -> bool:
        """Save downloaded image content to a file."""
        try:
            if not download_result['download_success'] or not download_result['file_content']:
                return False

            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'wb') as f:
                f.write(download_result['file_content'])

            return True

        except Exception as e:
            print(f"Error saving image to {output_path}: {str(e)}")
            return False

    def test_image_access(self, image_url: str) -> Dict[str, Any]:
        """
        Quick test to check if an image URL is accessible without downloading full content.
        """
        test_result = {
            'url': image_url,
            'accessible': False,
            'http_status': None,
            'content_type': None,
            'content_length': None,
            'error_message': None
        }

        try:
            # Build full URL
            if image_url.startswith('/'):
                full_url = urljoin(self.base_url, image_url)
            else:
                full_url = image_url

            # HEAD request to test accessibility
            response = self.session.head(full_url, timeout=10, verify=False)
            test_result['http_status'] = response.status_code
            test_result['content_type'] = response.headers.get('content-type')
            test_result['content_length'] = response.headers.get('content-length')

            if response.status_code == 200:
                test_result['accessible'] = True
            else:
                test_result['error_message'] = f"HTTP {response.status_code}"

        except Exception as e:
            test_result['error_message'] = str(e)

        return test_result