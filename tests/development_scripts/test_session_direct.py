#!/usr/bin/env python3
"""
Direct test of session-based image access with known URLs.
"""
import requests
import time

def test_session_direct():
    """Test session-based image access with manual URLs."""

    print("🔐 Direct Session Test")
    print("=" * 30)

    # Create session
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    })

    # Step 1: Load certificate page
    cert_url = "https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicFormDisplay&ttbid=24001001000001"
    print(f"📋 Loading certificate...")

    try:
        cert_response = session.get(cert_url, verify=False, timeout=30)
        print(f"✅ Certificate: HTTP {cert_response.status_code}")
        print(f"🍪 Cookies: {len(session.cookies)} found")

    except Exception as e:
        print(f"❌ Certificate failed: {str(e)}")
        return False

    # Step 2: Try image URLs immediately with session
    image_urls = [
        "/colasonline/publicViewAttachment.do?filename=Jourdan La Croix Boissee eff Jan 2024.jpg&filetype=l",
        "/colasonline/publicViewAttachment.do?filename=Jourdan Croix Boisee 2021 front label.jpg&filetype=l"
    ]

    print(f"\n🖼️  Testing {len(image_urls)} images...")

    success_count = 0
    for i, img_url in enumerate(image_urls):
        print(f"\n   Image {i+1}:")
        print(f"   URL: {img_url}")

        try:
            # Build full URL
            full_url = f"https://ttbonline.gov{img_url}"

            # Add referrer and image-specific headers
            headers = {
                'Referer': cert_url,
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Sec-Fetch-Dest': 'image',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'same-origin'
            }

            # Small delay to be respectful
            time.sleep(0.5)

            img_response = session.get(full_url, headers=headers, verify=False, timeout=30)

            print(f"   Status: HTTP {img_response.status_code}")
            print(f"   Content-Type: {img_response.headers.get('content-type', 'unknown')}")
            print(f"   Size: {len(img_response.content)} bytes")

            # Check if it's an image
            content_type = img_response.headers.get('content-type', '').lower()
            is_image = 'image/' in content_type
            is_large = len(img_response.content) > 1000

            if img_response.status_code == 200 and is_image and is_large:
                print(f"   ✅ SUCCESS - Valid image!")

                # Save it
                filename = f"/tmp/session_image_{i+1}.jpg"
                with open(filename, 'wb') as f:
                    f.write(img_response.content)
                print(f"   💾 Saved: {filename}")

                success_count += 1

            elif img_response.status_code == 200:
                print(f"   ❌ Not an image (Content-Type: {content_type})")
                # Show what we got instead
                if len(img_response.text) < 500:
                    print(f"   Response: {img_response.text[:200]}...")

            else:
                print(f"   ❌ HTTP Error: {img_response.status_code}")

        except Exception as e:
            print(f"   ❌ Failed: {str(e)}")

    # Results
    print(f"\n" + "=" * 30)
    if success_count > 0:
        print(f"🎉 SUCCESS! Downloaded {success_count}/{len(image_urls)} images")
        print(f"🚀 Session approach WORKS!")
        return True
    else:
        print(f"❌ No images downloaded")
        return False


if __name__ == "__main__":
    test_session_direct()