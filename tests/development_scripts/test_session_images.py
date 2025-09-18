#!/usr/bin/env python3
"""
Test session-based image downloads - load certificate first, then images.
"""
import sys
import requests
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from ciq_test_2.defs.ttb_data_extraction import parse_ttb_html


def test_session_images():
    """Test if images work with session from certificate page."""

    print("🔐 Testing Session-Based Image Access")
    print("=" * 45)

    # Create session
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })

    # Step 1: Load certificate page to establish session
    cert_url = "https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicFormDisplay&ttbid=24001001000001"
    print(f"📋 Step 1: Loading certificate page...")
    print(f"URL: {cert_url}")

    try:
        cert_response = session.get(cert_url, verify=False, timeout=30)
        print(f"✅ Certificate loaded: HTTP {cert_response.status_code}")
        print(f"Content-Type: {cert_response.headers.get('content-type')}")
        print(f"Content-Length: {len(cert_response.content)} bytes")

        # Check for session cookies
        cookies = session.cookies.get_dict()
        print(f"🍪 Session cookies: {len(cookies)} found")
        for name, value in cookies.items():
            print(f"   {name}: {value[:20]}...")

    except Exception as e:
        print(f"❌ Failed to load certificate: {str(e)}")
        return False

    # Step 2: Parse certificate to get image URLs
    print(f"\n📋 Step 2: Extracting image URLs...")
    try:
        cert_data = parse_ttb_html(cert_response.text, 'certificate')
        images = cert_data.get('label_images', [])
        print(f"✅ Found {len(images)} images")

    except Exception as e:
        print(f"❌ Failed to parse certificate: {str(e)}")
        return False

    if not images:
        print("❌ No images found to test")
        return False

    # Step 3: Attempt image downloads with established session
    print(f"\n🖼️  Step 3: Downloading images with session...")

    success_count = 0
    for i, img in enumerate(images):
        image_url = img.get('original_url')
        image_type = img.get('type', f'Image {i+1}')

        if not image_url:
            continue

        # Build full URL
        if image_url.startswith('/'):
            full_image_url = f"https://ttbonline.gov{image_url}"
        else:
            full_image_url = image_url

        print(f"\n   Testing: {image_type}")
        print(f"   URL: {image_url}")

        try:
            # Add referrer header
            headers = {
                'Referer': cert_url,
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8'
            }

            img_response = session.get(full_image_url, headers=headers, verify=False, timeout=30)

            print(f"   Status: HTTP {img_response.status_code}")
            print(f"   Content-Type: {img_response.headers.get('content-type')}")
            print(f"   Size: {len(img_response.content)} bytes")

            # Check if it's actually an image
            content_type = img_response.headers.get('content-type', '').lower()
            is_image = 'image/' in content_type
            is_large_enough = len(img_response.content) > 1000  # More than 1KB

            if img_response.status_code == 200 and is_image and is_large_enough:
                print(f"   ✅ SUCCESS - Valid image downloaded!")

                # Save the image
                filename = f"/tmp/session_test_{i+1}.jpg"
                with open(filename, 'wb') as f:
                    f.write(img_response.content)
                print(f"   💾 Saved to: {filename}")

                success_count += 1

            elif img_response.status_code == 200:
                print(f"   ❌ Downloaded but not an image (likely HTML)")
                # Show first 200 chars of response
                preview = img_response.text[:200].replace('\n', ' ')
                print(f"   Preview: {preview}...")

            else:
                print(f"   ❌ HTTP error: {img_response.status_code}")

        except Exception as e:
            print(f"   ❌ Request failed: {str(e)}")

    # Final results
    print(f"\n" + "=" * 45)
    success_rate = success_count / len(images) if images else 0

    if success_count > 0:
        print(f"🎉 SESSION APPROACH WORKS!")
        print(f"✅ Successfully downloaded {success_count}/{len(images)} images")
        print(f"📊 Success rate: {success_rate:.0%}")
        print(f"🚀 Full pipeline with images is POSSIBLE!")
        return True
    else:
        print(f"❌ Session approach failed")
        print(f"📊 Success rate: 0%")
        print(f"🔄 Images require different authentication approach")
        return False


if __name__ == "__main__":
    success = test_session_images()
    sys.exit(0 if success else 1)