"""BlurHash encoding helpers.

This is a small local copy of the blurhash encoding logic so Storage can
generate hashes without an additional runtime dependency.
"""

from __future__ import annotations

import math

alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz#$%*+,-.:;=?@[]^_{|}~"
alphabet_values = dict(zip(alphabet, range(len(alphabet))))


def base83_decode(base83_str: str) -> int:
    value = 0
    for base83_char in base83_str:
        value = value * 83 + alphabet_values[base83_char]
    return value


def base83_encode(value: int, length: int) -> str:
    if int(value) // (83**length) != 0:
        raise ValueError("Specified length is too short to encode given value.")

    result = ""
    for i in range(1, length + 1):
        digit = int(value) // (83 ** (length - i)) % 83
        result += alphabet[int(digit)]
    return result


def srgb_to_linear(value: int) -> float:
    value = float(value) / 255.0
    if value <= 0.04045:
        return value / 12.92
    return math.pow((value + 0.055) / 1.055, 2.4)


def sign_pow(value: float, exp: float) -> float:
    return math.copysign(math.pow(abs(value), exp), value)


def linear_to_srgb(value: float) -> int:
    value = max(0.0, min(1.0, value))
    if value <= 0.0031308:
        return int(value * 12.92 * 255 + 0.5)
    return int((1.055 * math.pow(value, 1 / 2.4) - 0.055) * 255 + 0.5)


def blurhash_encode(image, components_x: int = 4, components_y: int = 3, linear: bool = False) -> str:
    if components_x < 1 or components_x > 9 or components_y < 1 or components_y > 9:
        raise ValueError("x and y component counts must be between 1 and 9 inclusive.")

    height = float(len(image))
    width = float(len(image[0]))

    image_linear = []
    if not linear:
        for y in range(int(height)):
            image_linear_line = []
            for x in range(int(width)):
                image_linear_line.append(
                    [
                        srgb_to_linear(image[y][x][0]),
                        srgb_to_linear(image[y][x][1]),
                        srgb_to_linear(image[y][x][2]),
                    ]
                )
            image_linear.append(image_linear_line)
    else:
        image_linear = image

    components = []
    max_ac_component = 0.0
    for j in range(components_y):
        for i in range(components_x):
            norm_factor = 1.0 if (i == 0 and j == 0) else 2.0
            component = [0.0, 0.0, 0.0]
            for y in range(int(height)):
                for x in range(int(width)):
                    basis = norm_factor * math.cos(math.pi * float(i) * float(x) / width) * math.cos(
                        math.pi * float(j) * float(y) / height
                    )
                    component[0] += basis * image_linear[y][x][0]
                    component[1] += basis * image_linear[y][x][1]
                    component[2] += basis * image_linear[y][x][2]

            component[0] /= width * height
            component[1] /= width * height
            component[2] /= width * height
            components.append(component)

            if not (i == 0 and j == 0):
                max_ac_component = max(max_ac_component, abs(component[0]), abs(component[1]), abs(component[2]))

    dc_value = (linear_to_srgb(components[0][0]) << 16) + (linear_to_srgb(components[0][1]) << 8) + linear_to_srgb(
        components[0][2]
    )

    quant_max_ac_component = int(max(0, min(82, math.floor(max_ac_component * 166 - 0.5))))
    ac_component_norm_factor = float(quant_max_ac_component + 1) / 166.0

    ac_values = []
    for r, g, b in components[1:]:
        ac_values.append(
            int(max(0.0, min(18.0, math.floor(sign_pow(r / ac_component_norm_factor, 0.5) * 9.0 + 9.5))))
            * 19
            * 19
            + int(max(0.0, min(18.0, math.floor(sign_pow(g / ac_component_norm_factor, 0.5) * 9.0 + 9.5)))) * 19
            + int(max(0.0, min(18.0, math.floor(sign_pow(b / ac_component_norm_factor, 0.5) * 9.0 + 9.5))))
        )

    blurhash = ""
    blurhash += base83_encode((components_x - 1) + (components_y - 1) * 9, 1)
    blurhash += base83_encode(quant_max_ac_component, 1)
    blurhash += base83_encode(dc_value, 4)
    for ac_value in ac_values:
        blurhash += base83_encode(ac_value, 2)

    return blurhash
