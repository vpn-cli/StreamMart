import os
from PIL import Image, ImageDraw, ImageFont

def create_outage_gif(output_path="assets/demo-outage.gif"):
    width = 880
    height = 680
    bg_color = (24, 24, 37)       # #181825 Catppuccin Mocha Mantle
    header_color = (30, 30, 46)   # #1e1e2e Base
    border_color = (49, 50, 68)   # #313244 Surface0
    
    font_path = "C:/Windows/Fonts/consola.ttf"
    font_size = 14
    font = ImageFont.truetype(font_path, font_size)
    font_bold = ImageFont.truetype("C:/Windows/Fonts/consolab.ttf", font_size)
    font_title = ImageFont.truetype("C:/Windows/Fonts/consolab.ttf", 13)
    
    # Palette
    TEXT_DEFAULT = (205, 214, 244)  # #cdd6f4
    TEXT_DIM     = (108, 112, 134)  # #6c7086
    TEXT_GREEN   = (166, 227, 161)  # #a6e3a1
    TEXT_CYAN    = (137, 220, 235)  # #89dceb
    TEXT_RED     = (243, 139, 168)  # #f38ba8
    TEXT_YELLOW  = (249, 226, 175)  # #f9e2af
    TEXT_PEACH   = (250, 179, 135)  # #fab387
    TEXT_BLUE    = (137, 180, 250)  # #89b4fa
    
    # Script timeline segments (list of lines visible at each keyframe)
    # Each line is: (text, color, is_bold)
    
    steps = [
        # Frame 1: Terminal prompt
        [
            ("streammart@node-cluster:~$ npm run demo:outage", TEXT_CYAN, True),
        ],
        # Frame 2: Header banner
        [
            ("streammart@node-cluster:~$ npm run demo:outage", TEXT_CYAN, True),
            ("", TEXT_DEFAULT, False),
            ("============================================================================", TEXT_DIM, False),
            ("StreamMart Live Demo: Surviving a Complete Kafka Broker Outage", TEXT_YELLOW, True),
            ("============================================================================", TEXT_DIM, False),
            ("Transactional Outbox + Idempotent Kafka Consumer Resilience:", TEXT_PEACH, False),
            ("  1. Stop Kafka broker -> 2. Send 20 checkouts -> 3. Verify DB buffer", TEXT_DIM, False),
            ("  4. Restart Kafka broker -> 5. Watch zero-loss automatic drain & commit", TEXT_DIM, False),
        ],
        # Frame 3: Step 0 & Step 1
        [
            ("streammart@node-cluster:~$ npm run demo:outage", TEXT_CYAN, True),
            ("", TEXT_DEFAULT, False),
            ("============================================================================", TEXT_DIM, False),
            ("StreamMart Live Demo: Surviving a Complete Kafka Broker Outage", TEXT_YELLOW, True),
            ("============================================================================", TEXT_DIM, False),
            ("[Step 0] Checking Docker stack health...", TEXT_BLUE, False),
            ("[OK] Stack is healthy (API Gateway @ :8085, Postgres @ :5432, Kafka healthy)", TEXT_GREEN, False),
            ("", TEXT_DEFAULT, False),
            ("[Step 1] Minting 20 shopper JWTs via /api/auth/token...", TEXT_BLUE, False),
            ("[OK] Ready with 20 demo shopper tokens", TEXT_GREEN, False),
        ],
        # Frame 4: Step 2 Pull plug
        [
            ("streammart@node-cluster:~$ npm run demo:outage", TEXT_CYAN, True),
            ("", TEXT_DEFAULT, False),
            ("============================================================================", TEXT_DIM, False),
            ("StreamMart Live Demo: Surviving a Complete Kafka Broker Outage", TEXT_YELLOW, True),
            ("============================================================================", TEXT_DIM, False),
            ("[Step 0] Stack is healthy (API Gateway @ :8085, Kafka healthy)", TEXT_GREEN, False),
            ("[Step 1] Ready with 20 demo shopper tokens", TEXT_GREEN, False),
            ("", TEXT_DEFAULT, False),
            ("[Step 2] Pulling the plug on Kafka broker (docker stop streammart-kafka)...", TEXT_RED, True),
            ("[OFFLINE] Apache Kafka is now DOWN", TEXT_RED, True),
            ("  * payment-service /health (liveness):  HTTP 200 (checkout API stays UP)", TEXT_GREEN, False),
            ("  * payment-service /relay-status:       HTTP 503 (relay safely paused)", TEXT_YELLOW, False),
        ],
        # Frame 5: Step 3 Checkouts during outage
        [
            ("streammart@node-cluster:~$ npm run demo:outage", TEXT_CYAN, True),
            ("", TEXT_DEFAULT, False),
            ("============================================================================", TEXT_DIM, False),
            ("StreamMart Live Demo: Surviving a Complete Kafka Broker Outage", TEXT_YELLOW, True),
            ("============================================================================", TEXT_DIM, False),
            ("[Step 2] [OFFLINE] Kafka is DOWN | payment-service /health: HTTP 200", TEXT_RED, False),
            ("", TEXT_DEFAULT, False),
            ("[Step 3] Sending 20 customer checkouts while Kafka is completely DOWN...", TEXT_BLUE, False),
            ("[SUCCESS] Accepted 20/20 checkouts with HTTP 200 OK in 118ms!", TEXT_GREEN, True),
            ("", TEXT_DEFAULT, False),
            ("[Step 4] Inspecting PostgreSQL while Kafka is STILL OFFLINE:", TEXT_BLUE, False),
            ("  - Payments committed in Postgres:            20 / 20", TEXT_DEFAULT, False),
            ("  - Queued outbox rows (published = false):    20 / 20", TEXT_YELLOW, True),
            ("  - Orders created while Kafka is down:         0 / 20 (safely buffered)", TEXT_CYAN, False),
        ],
        # Frame 6: Step 5 Restart Kafka & Drain in progress
        [
            ("streammart@node-cluster:~$ npm run demo:outage", TEXT_CYAN, True),
            ("", TEXT_DEFAULT, False),
            ("============================================================================", TEXT_DIM, False),
            ("StreamMart Live Demo: Surviving a Complete Kafka Broker Outage", TEXT_YELLOW, True),
            ("============================================================================", TEXT_DIM, False),
            ("[Step 3] Accepted 20/20 checkouts (HTTP 200) during broker outage", TEXT_GREEN, False),
            ("[Step 4] Postgres Outbox buffer: 20 pending events (0 orders while down)", TEXT_YELLOW, False),
            ("", TEXT_DEFAULT, False),
            ("[Step 5] Restarting Kafka broker (docker start streammart-kafka)...", TEXT_BLUE, False),
            (">> Waiting for Kafka healthcheck............ healthy!", TEXT_GREEN, False),
            ("", TEXT_DEFAULT, False),
            ("[Step 6] Outbox relay reconnected: draining queued checkouts...", TEXT_BLUE, False),
            ("   Outbox remaining:  8 | Orders created: 12/20 | Emails sent: 12/20", TEXT_YELLOW, False),
        ],
        # Frame 7: Drain completed
        [
            ("streammart@node-cluster:~$ npm run demo:outage", TEXT_CYAN, True),
            ("", TEXT_DEFAULT, False),
            ("============================================================================", TEXT_DIM, False),
            ("StreamMart Live Demo: Surviving a Complete Kafka Broker Outage", TEXT_YELLOW, True),
            ("============================================================================", TEXT_DIM, False),
            ("[Step 5] Kafka restarted & healthy", TEXT_GREEN, False),
            ("[Step 6] Outbox relay reconnected: draining queued checkouts...", TEXT_BLUE, False),
            ("   Outbox remaining:  0 | Orders created: 20/20 | Emails sent: 20/20", TEXT_GREEN, True),
            ("[SUCCESS] Full recovery & end-to-end delivery completed in 1.42s!", TEXT_GREEN, True),
            ("", TEXT_DEFAULT, False),
            ("============================================================================", TEXT_DIM, False),
            ("DEMO SUMMARY & VERIFICATION", TEXT_YELLOW, True),
            ("============================================================================", TEXT_DIM, False),
            ("   Checkouts sent while Kafka was DOWN:   20", TEXT_DEFAULT, False),
            ("   HTTP 200 accepted during outage:       20 / 20 (100%)", TEXT_GREEN, True),
            ("   Orders created after Kafka recovery:   20 / 20 (100%)", TEXT_GREEN, True),
            ("   Emails queued after Kafka recovery:    20 / 20 (100%)", TEXT_GREEN, True),
            ("   Orders lost:                           0 (ZERO DATA LOSS)", TEXT_GREEN, True),
            ("   Duplicate orders:                      0 (100% IDEMPOTENT)", TEXT_GREEN, True),
            ("   payment-service /relay-status now:     HTTP 200 OK", TEXT_GREEN, False),
            ("============================================================================", TEXT_DIM, False),
        ],
    ]
    
    # Durations in ms for each step
    durations = [1200, 1600, 1600, 2000, 2200, 1800, 5000]
    
    images = []
    
    for step_lines in steps:
        img = Image.new("RGB", (width, height), bg_color)
        draw = ImageDraw.Draw(img)
        
        # Outer Border
        draw.rectangle([(0, 0), (width - 1, height - 1)], outline=border_color, width=1)
        
        # Window Header
        draw.rectangle([(0, 0), (width, 38)], fill=header_color)
        draw.line([(0, 38), (width, 38)], fill=border_color, width=1)
        
        # Window Control Dots (macOS style)
        draw.ellipse([(14, 13), (26, 25)], fill=(243, 139, 168)) # Red
        draw.ellipse([(34, 13), (46, 25)], fill=(249, 226, 175)) # Yellow
        draw.ellipse([(54, 13), (66, 25)], fill=(166, 227, 161)) # Green
        
        # Terminal Title
        title_text = "streammart - demo:outage (Transactional Outbox Verification)"
        draw.text((width // 2 - 190, 12), title_text, fill=TEXT_DIM, font=font_title)
        
        # Content Lines
        x_margin = 24
        y_cursor = 56
        line_height = 21
        
        for line_text, color, is_bold in step_lines:
            f = font_bold if is_bold else font
            draw.text((x_margin, y_cursor), line_text, fill=color, font=f)
            y_cursor += line_height
            
        images.append(img)
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    images[0].save(
        output_path,
        save_all=True,
        append_images=images[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    print(f"Generated outage GIF at {output_path} ({os.path.getsize(output_path)} bytes)")

if __name__ == "__main__":
    create_outage_gif()
