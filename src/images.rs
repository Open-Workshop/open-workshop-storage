use std::path::Path;

use blurhash::encode;
use image::codecs::webp::WebPEncoder;
use image::{ColorType, DynamicImage, GenericImageView, ImageError};

pub fn image_bytes_to_webp(data: &[u8], _quality: u8) -> Result<Vec<u8>, ImageError> {
    let img = image::load_from_memory(data)?;
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
    let img = image::load_from_memory(data)?;
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
