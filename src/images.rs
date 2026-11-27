use std::path::Path;
use std::io::Cursor;

use blurhash::encode;
use image::codecs::webp::WebPEncoder;
use image::{ColorType, DynamicImage, GenericImageView, ImageError};

// image 0.24's WebP decoder rejects or panics on some valid images. Decode
// WebP with the maintained codec without changing the PNG and JPEG codecs.
fn decode_webp(data: &[u8]) -> Option<DynamicImage> {
    const MAX_OUTPUT_BYTES: usize = 512 * 1024 * 1024;
    let mut decoder = image_webp::WebPDecoder::new(Cursor::new(data)).ok()?;
    decoder.set_memory_limit(MAX_OUTPUT_BYTES);
    let (width, height) = decoder.dimensions();
    let has_alpha = decoder.has_alpha();
    let output_size = decoder.output_buffer_size()?;
    if output_size > MAX_OUTPUT_BYTES {
        return None;
    }
    let mut pixels = vec![0; output_size];
    decoder.read_image(&mut pixels).ok()?;
    if has_alpha {
        image::RgbaImage::from_raw(width, height, pixels).map(DynamicImage::ImageRgba8)
    } else {
        image::RgbImage::from_raw(width, height, pixels).map(DynamicImage::ImageRgb8)
    }
}

fn decode_image(data: &[u8]) -> Result<DynamicImage, ImageError> {
    if matches!(image::guess_format(data), Ok(image::ImageFormat::WebP)) {
        return decode_webp(data).ok_or_else(|| {
            ImageError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WebP decoding failed or image exceeds the memory limit",
            ))
        });
    }
    image::load_from_memory(data)
}

pub fn image_bytes_to_webp(data: &[u8], _quality: u8) -> Result<Vec<u8>, ImageError> {
    let img = decode_image(data)?;
    let rgba = img.to_rgba8();
    let mut out = Vec::new();
    let encoder = WebPEncoder::new_lossless(&mut out);
    encoder.encode(rgba.as_raw(), rgba.width(), rgba.height(), ColorType::Rgba8)?;
    Ok(out)
}

pub fn image_file_to_webp(
    src_path: impl AsRef<Path>,
    dst_path: impl AsRef<Path>,
    quality: u8,
) -> Result<(), ImageError> {
    let data = std::fs::read(src_path)?;
    let converted = image_bytes_to_webp(&data, quality)?;
    std::fs::write(dst_path, converted)?;
    Ok(())
}

pub fn image_bytes_to_blurhash(
    data: &[u8],
    components_x: u32,
    components_y: u32,
    max_dimension: u32,
) -> Result<(String, u32, u32), ImageError> {
    let img = decode_image(data)?;
    let (width, height) = img.dimensions();
    let mut rgba = img.to_rgba8();
    if width.max(height) > max_dimension {
        let dyn_img = DynamicImage::ImageRgba8(rgba);
        let resized = dyn_img.thumbnail(max_dimension, max_dimension);
        rgba = resized.to_rgba8();
    }
    let (encode_width, encode_height) = rgba.dimensions();

    let mut flattened = Vec::with_capacity((rgba.width() * rgba.height() * 4) as usize);
    for pixel in rgba.pixels() {
        let [r, g, b, a] = pixel.0;
        if a == 255 {
            flattened.extend_from_slice(&[r, g, b, 255]);
            continue;
        }
        let alpha = a as u16;
        let inv_alpha = 255u16.saturating_sub(alpha);
        let rr = ((r as u16 * alpha + 255u16 * inv_alpha) / 255) as u8;
        let gg = ((g as u16 * alpha + 255u16 * inv_alpha) / 255) as u8;
        let bb = ((b as u16 * alpha + 255u16 * inv_alpha) / 255) as u8;
        flattened.extend_from_slice(&[rr, gg, bb, 255]);
    }

    let blurhash = encode(
        components_x,
        components_y,
        encode_width,
        encode_height,
        &flattened,
    )
    .map_err(|err| {
        ImageError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            err.to_string(),
        ))
    })?;
    Ok((blurhash, width, height))
}

pub fn image_file_to_blurhash(
    src_path: impl AsRef<Path>,
    components_x: u32,
    components_y: u32,
    max_dimension: u32,
) -> Result<(String, u32, u32), ImageError> {
    let data = std::fs::read(src_path)?;
    image_bytes_to_blurhash(&data, components_x, components_y, max_dimension)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webp_decoder_preserves_transparency() {
        let pixels = [20, 40, 60, 0, 120, 140, 160, 128, 220, 230, 240, 255, 1, 2, 3, 64];
        let mut bytes = Vec::new();
        WebPEncoder::new_lossless(&mut bytes).encode(&pixels, 2, 2, ColorType::Rgba8).unwrap();
        let decoded = decode_webp(&bytes).unwrap().to_rgba8();
        assert_eq!(decoded.dimensions(), (2, 2));
        // The RGB channels of a fully transparent pixel may be normalized.
        assert_eq!(decoded.get_pixel(0, 0).0[3], 0);
        assert_eq!(decoded.get_pixel(1, 0).0, [120, 140, 160, 128]);
        assert_eq!(decoded.get_pixel(0, 1).0, [220, 230, 240, 255]);
    }

    #[test]
    fn webp_decoder_rejects_invalid_webp() {
        assert!(decode_webp(b"RIFF\x00\x00\x00\x00WEBP").is_none());
    }

    #[test]
    fn decodes_upstream_color_index_regression() {
        let bytes = include_bytes!("../tests/fixtures/webp-color-index.webp");
        assert!(image_bytes_to_blurhash(bytes, 4, 3, 64).is_ok());
    }
}
