#!/usr/bin/env python3
"""
Create simple PNG icons for the Chrome extension
Requires PIL/Pillow: pip install Pillow
"""

try:
    from PIL import Image, ImageDraw, ImageFont
    import os

    def create_icon(size):
        # Create image with gradient background
        img = Image.new('RGB', (size, size), color='white')
        draw = ImageDraw.Draw(img)

        # Draw gradient background (purple theme)
        for y in range(size):
            color_val = int(102 + (118 - 102) * (y / size))
            color = (color_val, 126, 234)
            draw.line([(0, y), (size, y)], fill=color)

        # Draw paw print emoji/symbol
        # Simple circles for paw representation
        pad_size = size // 10

        # Main pad (larger circle at bottom)
        main_x = size // 2
        main_y = int(size * 0.65)
        main_r = int(size * 0.25)
        draw.ellipse([main_x - main_r, main_y - main_r,
                      main_x + main_r, main_y + main_r],
                     fill='white')

        # Four toe pads (smaller circles)
        toe_r = int(size * 0.12)
        toe_positions = [
            (int(size * 0.28), int(size * 0.38)),  # Left toe
            (int(size * 0.45), int(size * 0.28)),  # Left-center toe
            (int(size * 0.62), int(size * 0.32)),  # Right-center toe
            (int(size * 0.75), int(size * 0.42)),  # Right toe
        ]

        for toe_x, toe_y in toe_positions:
            draw.ellipse([toe_x - toe_r, toe_y - toe_r,
                         toe_x + toe_r, toe_y + toe_r],
                        fill='white')

        return img

    # Create icons in different sizes
    sizes = [16, 48, 128]

    script_dir = os.path.dirname(os.path.abspath(__file__))

    for size in sizes:
        icon = create_icon(size)
        icon_path = os.path.join(script_dir, f'icon{size}.png')
        icon.save(icon_path, 'PNG')
        print(f"✓ Created {icon_path}")

    print("\n✓ All icons created successfully!")

except ImportError:
    print("ERROR: Pillow library not found")
    print("Install with: pip install Pillow")
    print("\nOr use the provided placeholder icons")
