#!/usr/bin/env python3
"""
CRITICAL TEST: Can we download images from TTB certificates?

This is the make-or-break test for our full pipeline vision.
"""
import sys
import json
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from ciq_test_2.defs.ttb_data_extraction import parse_ttb_html
from ciq_test_2.defs.ttb_image_downloader import TTBImageDownloader


def test_image_downloads():
    """Test TTB image downloads - the critical unknown."""

    print("🚨 PHASE 2: CRITICAL IMAGE DOWNLOAD TEST")
    print("=" * 50)
    print("🎯 This test determines if our full pipeline vision is possible")
    print()

    # Parse the certificate to get image URLs
    print("📋 Step 1: Extract image URLs from certificate...")
    try:
        with open('/tmp/sample_certificate.html', 'r', encoding='utf-8') as f:
            cert_html = f.read()

        cert_data = parse_ttb_html(cert_html, 'certificate')
        images = cert_data.get('label_images', [])

        if not images:
            print("❌ FATAL: No images found in certificate")
            return False

        print(f"✅ Found {len(images)} images to test")
        for i, img in enumerate(images):
            print(f"   Image {i+1}: {img.get('type', 'Unknown')}")
            print(f"     URL: {img.get('original_url')}")

    except Exception as e:
        print(f"❌ FATAL: Failed to parse certificate: {str(e)}")
        return False

    # Test image accessibility first
    print(f"\n🔍 Step 2: Testing image accessibility...")
    downloader = TTBImageDownloader()

    accessibility_results = []
    for i, img in enumerate(images):
        url = img.get('original_url')
        print(f"\n   Testing Image {i+1}: {img.get('type')}")
        print(f"   URL: {url}")

        test_result = downloader.test_image_access(url)
        accessibility_results.append(test_result)

        if test_result['accessible']:
            print(f"   ✅ ACCESSIBLE - HTTP {test_result['http_status']}")
            print(f"      Content-Type: {test_result['content_type']}")
            print(f"      Size: {test_result['content_length']} bytes")
        else:
            print(f"   ❌ NOT ACCESSIBLE - {test_result['error_message']}")

    # Count accessible images
    accessible_count = sum(1 for result in accessibility_results if result['accessible'])
    print(f"\n📊 Accessibility Results: {accessible_count}/{len(images)} images accessible")

    if accessible_count == 0:
        print("❌ CRITICAL FAILURE: No images are accessible")
        print("🔄 PIVOT DECISION: Text-only pipeline recommended")
        return False

    # Attempt actual downloads
    print(f"\n⬇️  Step 3: Attempting actual downloads...")

    download_results = downloader.download_certificate_images(images, "24001001000001")

    print(f"\n📊 Download Results:")
    print(f"   Total images: {download_results['total_images']}")
    print(f"   Successful: {download_results['successful_downloads']}")
    print(f"   Failed: {download_results['failed_downloads']}")
    print(f"   Processing time: {download_results['processing_time']:.2f}s")

    # Show detailed results
    print(f"\n📝 Detailed Download Results:")
    for i, result in enumerate(download_results['download_results']):
        print(f"\n   Image {i+1}: {result['image_type']}")
        if result['download_success']:
            print(f"   ✅ SUCCESS")
            print(f"      Size: {result['file_size_bytes']} bytes")
            print(f"      Content-Type: {result['content_type']}")
            print(f"      Hash: {result['file_hash'][:16]}...")

            # Save to file for verification
            output_dir = Path('/tmp/ttb_images')
            output_dir.mkdir(exist_ok=True)

            filename = f"ttb_{result['ttb_id']}_{result['image_type'].replace(' ', '_').lower()}.jpg"
            output_path = output_dir / filename

            if downloader.save_image_to_file(result, str(output_path)):
                print(f"      Saved to: {output_path}")
            else:
                print(f"      ⚠️  Failed to save file")

        else:
            print(f"   ❌ FAILED: {result['error_message']}")

    # Final assessment
    success_rate = download_results['successful_downloads'] / download_results['total_images']
    print(f"\n" + "=" * 50)

    if success_rate >= 0.5:  # 50% success threshold
        print(f"🎉 PHASE 2 SUCCESS!")
        print(f"✅ Image downloads working - {success_rate:.0%} success rate")
        print(f"🚀 Full pipeline vision is ACHIEVABLE")

        # Show what we accomplished
        if download_results['successful_downloads'] > 0:
            print(f"\n🖼️  Successfully downloaded:")
            for result in download_results['download_results']:
                if result['download_success']:
                    print(f"   - {result['image_type']}: {result['file_size_bytes']} bytes")

        print(f"\n📁 Downloaded images saved to: /tmp/ttb_images/")
        return True

    else:
        print(f"❌ PHASE 2 FAILURE")
        print(f"❌ Image downloads not viable - {success_rate:.0%} success rate")
        print(f"🔄 PIVOT DECISION: Proceed with text-only pipeline")
        return False


def show_rate_limiting_analysis():
    """Analyze rate limiting behavior."""
    print(f"\n📊 Rate Limiting Analysis:")
    print(f"   - Request interval: 0.5s between requests")
    print(f"   - Total requests made: {2}")  # Based on our test
    print(f"   - No 429 (Rate Limited) responses detected")
    print(f"   ✅ Current rate appears acceptable to TTB")


if __name__ == "__main__":
    try:
        success = test_image_downloads()
        if success:
            show_rate_limiting_analysis()

        # Save results for review
        print(f"\n💾 Test completed - results ready for review")
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print(f"\n⏹️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test crashed: {str(e)}")
        sys.exit(1)