"""
P2P Dataset Generator v2
========================
Generates linked Purchase Orders, Goods Receipts, and Invoices as PDFs.
All three documents share the same PO number — exactly what the 3-way
match pipeline needs.

Changes from v1:
  - All vendor names are fully fictional (no real company names)
  - Dynamic multi-line items per document (1 to 4 line items per PO/GR/Invoice)
  - Ground truth captures all line items as JSON

Scenarios:
  MATCH_EXACT               - perfect three-way match
  MATCH_TOLERANCE           - invoice price within ±5% of PO
  MATCH_PARTIAL_DELIVERY    - GR qty < PO qty, invoice matches GR
  EXC_QTY_MISMATCH          - invoice qty > GR received qty
  EXC_PRICE_MISMATCH        - invoice price > 5% above PO
  EXC_WRONG_VENDOR          - invoice vendor differs from PO vendor
  EXC_DUPLICATE             - same invoice submitted twice
  EXC_GR_DATE_AFTER_INVOICE - invoice date before goods received

Usage:
    python generate_p2p_dataset_v2.py

Output:
    ./dataset_v2/purchase_order/   PO PDFs
    ./dataset_v2/good_receipt/     GR PDFs
    ./dataset_v2/invoice/          Invoice PDFs
    ./dataset_v2/ground_truth.csv  Golden eval labels
"""

import os, csv, json, random
from datetime import date, timedelta
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER

random.seed(42)

# ── Output folders ─────────────────────────────────────────────────────────────
BASE   = "./dataset_v2"
PO_DIR = f"{BASE}/purchase_order"
GR_DIR = f"{BASE}/good_receipt"
IV_DIR = f"{BASE}/invoice"
for d in [PO_DIR, GR_DIR, IV_DIR]:
    os.makedirs(d, exist_ok=True)

# ── Fully fictional vendors ────────────────────────────────────────────────────
VENDORS = [
    {"vendor_id": "V001", "name": "Zephyr Trading LLC",
     "address": "Unit 4B, Industrial Zone A, Dubai, UAE",
     "trn": "100312200300001"},
    {"vendor_id": "V002", "name": "Orion Supply Group FZ LLC",
     "address": "Block 7, Free Zone West, Abu Dhabi, UAE",
     "trn": "100198700400002"},
    {"vendor_id": "V003", "name": "Crestline Procurement LLC",
     "address": "Warehouse 12, Logistics Park, Sharjah, UAE",
     "trn": "100445500200003"},
    {"vendor_id": "V004", "name": "Meridian Commerce FZ LLC",
     "address": "Suite 302, Trade Tower, Fujairah, UAE",
     "trn": "100567800100004"},
    {"vendor_id": "V005", "name": "Pinnacle Industrial Supply LLC",
     "address": "Plot 9, Industrial Estate, Ras Al Khaimah, UAE",
     "trn": "100634400300005"},
    {"vendor_id": "V006", "name": "Solaris Global Trading LLC",
     "address": "Office 15, Commercial Hub, Ajman, UAE",
     "trn": "100721100200006"},
    {"vendor_id": "V007", "name": "Vantage Logistics FZ LLC",
     "address": "Gate 3, Port Zone, Umm Al Quwain, UAE",
     "trn": "100883300400007"},
]

# ── Fictional materials ────────────────────────────────────────────────────────
MATERIALS = [
    {"material_id": "M001", "name": "Portable Work Station Model X200",
     "category": "IT Hardware",      "unit": "PCS",  "unit_price": 3200},
    {"material_id": "M002", "name": "Laser Document Printer LX-400",
     "category": "IT Hardware",      "unit": "PCS",  "unit_price": 850},
    {"material_id": "M003", "name": "Heavy Duty Safety Helmet Type-B",
     "category": "Safety Equipment", "unit": "PCS",  "unit_price": 45},
    {"material_id": "M004", "name": "Structured Network Cable Cat6 100m",
     "category": "Networking",       "unit": "ROLL", "unit_price": 120},
    {"material_id": "M005", "name": "Open Frame Server Cabinet 42U",
     "category": "IT Hardware",      "unit": "PCS",  "unit_price": 1800},
    {"material_id": "M006", "name": "Ergonomic Executive Office Chair",
     "category": "Furniture",        "unit": "PCS",  "unit_price": 380},
    {"material_id": "M007", "name": "Uninterruptible Power Supply 2KVA",
     "category": "Power Systems",    "unit": "PCS",  "unit_price": 650},
    {"material_id": "M008", "name": "High-Speed Video Cable 2m",
     "category": "Accessories",      "unit": "PCS",  "unit_price": 25},
    {"material_id": "M009", "name": "Industrial Air Filtration Unit",
     "category": "HVAC",             "unit": "PCS",  "unit_price": 2100},
    {"material_id": "M010", "name": "Wireless Access Point Pro Series",
     "category": "Networking",       "unit": "PCS",  "unit_price": 310},
    {"material_id": "M011", "name": "Fire Extinguisher CO2 5KG",
     "category": "Safety Equipment", "unit": "PCS",  "unit_price": 95},
    {"material_id": "M012", "name": "LED Panel Light 60W",
     "category": "Electrical",       "unit": "PCS",  "unit_price": 75},
]

# ── Buyer (fixed fictional company) ───────────────────────────────────────────
BUYER = {
    "name":    "Aurelius Corporation LLC",
    "address": "Floor 12, Nexus Business Tower, Dubai, UAE",
    "trn":     "100234567890001",
    "bank":    "Vantage Bank UAE",
    "iban":    "AE07 0991 2345 6789 0123 456",
    "swift":   "VBKLAEAD",
}

# ── PDF Styling ────────────────────────────────────────────────────────────────
H1  = ParagraphStyle("h1",  fontSize=16, fontName="Helvetica-Bold", spaceAfter=3)
H2  = ParagraphStyle("h2",  fontSize=10, fontName="Helvetica-Bold", spaceAfter=2)
SM  = ParagraphStyle("sm",  fontSize=8.5,fontName="Helvetica",      spaceAfter=1, leading=12)
CTR = ParagraphStyle("ctr", fontSize=7.5,fontName="Helvetica",      alignment=TA_CENTER,
                     textColor=colors.HexColor("#888888"))

HEADER_COLOR = colors.HexColor("#1a3c5e")
ALT_COLOR    = colors.HexColor("#f0f4f8")
BORDER_COLOR = colors.HexColor("#b0c0d0")

def tbl_style(has_total_row=False):
    s = [
        ("BACKGROUND",    (0, 0), (-1,  0), HEADER_COLOR),
        ("TEXTCOLOR",     (0, 0), (-1,  0), colors.white),
        ("FONTNAME",      (0, 0), (-1,  0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1,  0), 8),
        ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",      (0, 1), (-1, -1), 8),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, ALT_COLOR]),
        ("GRID",          (0, 0), (-1, -1), 0.25, BORDER_COLOR),
        ("ALIGN",         (3, 0), (-1, -1), "RIGHT"),
        ("ALIGN",         (3, 0), (-1,  0), "RIGHT"),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]
    if has_total_row:
        s += [
            ("FONTNAME",   (0, -1), (-1, -1), "Helvetica-Bold"),
            ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#dce8f0")),
            ("LINEABOVE",  (0, -1), (-1, -1), 0.5, HEADER_COLOR),
        ]
    return TableStyle(s)

def page_w():
    return A4[0] - 40 * mm

def build(path, story):
    doc = SimpleDocTemplate(path, pagesize=A4,
                            leftMargin=20*mm, rightMargin=20*mm,
                            topMargin=18*mm, bottomMargin=18*mm)
    doc.build(story)

def divider():
    return Table([[""]], colWidths=[page_w()],
                 style=TableStyle([("LINEBELOW",(0,0),(0,0), 0.5, HEADER_COLOR)]))

def header_block(left_pairs, right_pairs):
    """Two-column meta block: list of (label, value) tuples on each side."""
    def fmt(pairs):
        return "\n".join(f"<b>{k}</b>  {v}" for k, v in pairs)
    t = Table(
        [[Paragraph(fmt(left_pairs), SM), Paragraph(fmt(right_pairs), SM)]],
        colWidths=[page_w() * 0.52, page_w() * 0.48]
    )
    t.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP")]))
    return t

def totals_block(subtotal, vat, total):
    """Right-aligned totals section."""
    w = page_w()
    rows = [
        ["", "Subtotal:",   f"AED {subtotal:,.2f}"],
        ["", "VAT (5%):",   f"AED {vat:,.2f}"],
        ["", "Total:",      f"AED {total:,.2f}"],
    ]
    t = Table(rows, colWidths=[w*0.42, w*0.33, w*0.25])
    t.setStyle(TableStyle([
        ("ALIGN",        (1, 0), (-1, -1), "RIGHT"),
        ("FONTNAME",     (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE",     (0, 0), (-1, -1), 8.5),
        ("FONTNAME",     (0, 2), (-1,  2), "Helvetica-Bold"),
        ("LINEABOVE",    (1, 2), (-1,  2), 0.5, colors.black),
        ("TOPPADDING",   (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 2),
    ]))
    return t

# ── Line item helpers ─────────────────────────────────────────────────────────
def pick_line_items(n_lines=None, exclude_ids=None):
    """Pick n_lines distinct materials (default: 1–4 random)."""
    if n_lines is None:
        n_lines = random.choices([1, 2, 3, 4], weights=[30, 40, 20, 10])[0]
    pool = [m for m in MATERIALS if (exclude_ids is None or m["material_id"] not in exclude_ids)]
    chosen = random.sample(pool, min(n_lines, len(pool)))
    return [{"material": m, "qty": random.randint(2, 40)} for m in chosen]

def compute_totals(line_items, price_override=None):
    """Compute subtotal / vat / total from line items."""
    subtotal = sum(
        (price_override if price_override else li["material"]["unit_price"]) * li["qty"]
        for li in line_items
    )
    vat   = round(subtotal * 0.05, 2)
    total = round(subtotal + vat, 2)
    return subtotal, vat, total

# ── PO PDF ────────────────────────────────────────────────────────────────────
def gen_po(po_num, vendor, line_items, po_date, delivery_date, path):
    subtotal, vat, total = compute_totals(line_items)

    # Line items table rows
    rows = [["#", "Material ID", "Description", "Category", "Unit", "Qty",
             "Unit Price\n(AED)", "Line Total\n(AED)"]]
    for i, li in enumerate(line_items, 1):
        m   = li["material"]
        qty = li["qty"]
        rows.append([
            str(i), m["material_id"], m["name"], m["category"],
            m["unit"], str(qty),
            f"{m['unit_price']:,.2f}",
            f"{m['unit_price']*qty:,.2f}"
        ])
    rows.append(["", "", "", "", "", "", "TOTAL", f"{subtotal:,.2f}"])

    col_w = [8, 20, 52, 25, 12, 10, 22, 22]
    col_w = [c * mm for c in col_w]

    story = [
        Paragraph("PURCHASE ORDER", H1),
        Paragraph(f"{BUYER['name']}  |  {BUYER['address']}  |  TRN: {BUYER['trn']}", SM),
        Spacer(1, 3*mm), divider(), Spacer(1, 3*mm),
        header_block(
            [("PO Number:",    po_num),
             ("PO Date:",      str(po_date)),
             ("Delivery By:",  str(delivery_date)),
             ("Payment Terms:","Net 30"),
             ("Currency:",     "AED")],
            [("Vendor:",       vendor["name"]),
             ("Vendor ID:",    vendor["vendor_id"]),
             ("Address:",      vendor["address"]),
             ("TRN:",          vendor["trn"])]
        ),
        Spacer(1, 5*mm),
        Paragraph("Line Items", H2),
        Table(rows, colWidths=col_w, style=tbl_style(has_total_row=True)),
        Spacer(1, 3*mm),
        totals_block(subtotal, vat, total),
        Spacer(1, 6*mm),
        Paragraph("Approved by: Procurement Department — Aurelius Corporation LLC", SM),
        Spacer(1, 3*mm),
        Paragraph("This is a computer-generated purchase order for portfolio/demo purposes only.", CTR),
    ]
    build(path, story)

    return {
        "po_number": po_num, "vendor_name": vendor["name"],
        "vendor_id": vendor["vendor_id"], "po_date": str(po_date),
        "delivery_date": str(delivery_date),
        "line_items": [{"material_id": li["material"]["material_id"],
                        "material_name": li["material"]["name"],
                        "qty": li["qty"],
                        "unit_price": li["material"]["unit_price"]} for li in line_items],
        "total_aed": total
    }

# ── GR PDF ────────────────────────────────────────────────────────────────────
def gen_gr(gr_num, po_num, vendor, line_items, gr_date, gr_status, path,
           received_qty_override=None):
    """
    received_qty_override: dict of {material_id: received_qty} for partial deliveries.
    If None, all lines are received in full.
    """
    rows = [["#", "Material ID", "Description", "Unit",
             "Ordered Qty", "Received Qty", "Condition"]]
    for i, li in enumerate(line_items, 1):
        m   = li["material"]
        ord_qty = li["qty"]
        rec_qty = (received_qty_override or {}).get(m["material_id"], ord_qty)
        rows.append([str(i), m["material_id"], m["name"], m["unit"],
                     str(ord_qty), str(rec_qty), "Good"])

    col_w = [8, 20, 60, 12, 20, 22, 22]
    col_w = [c * mm for c in col_w]

    story = [
        Paragraph("GOODS RECEIPT", H1),
        Paragraph(f"{BUYER['name']}  |  Warehouse Operations  |  {BUYER['address']}", SM),
        Spacer(1, 3*mm), divider(), Spacer(1, 3*mm),
        header_block(
            [("GR Number:",    gr_num),
             ("GR Date:",      str(gr_date)),
             ("PO Reference:", po_num),
             ("GR Status:",    gr_status)],
            [("Vendor:",       vendor["name"]),
             ("Vendor ID:",    vendor["vendor_id"]),
             ("Address:",      vendor["address"]),
             ("Received by:",  "Warehouse Team")]
        ),
        Spacer(1, 5*mm),
        Paragraph("Received Items", H2),
        Table(rows, colWidths=col_w, style=tbl_style()),
        Spacer(1, 6*mm),
        Paragraph(f"GR Status: <b>{gr_status}</b> — Items inspected and accepted per above quantities.", SM),
        Spacer(1, 4*mm),
        Paragraph("Received by: ________________________   Signature: ___________   Date: ___________", SM),
        Spacer(1, 3*mm),
        Paragraph("This is a computer-generated goods receipt for portfolio/demo purposes only.", CTR),
    ]
    build(path, story)

    received = []
    for li in line_items:
        m = li["material"]
        rec_qty = (received_qty_override or {}).get(m["material_id"], li["qty"])
        received.append({"material_id": m["material_id"],
                         "material_name": m["name"],
                         "ordered_qty": li["qty"],
                         "received_qty": rec_qty})
    return {
        "gr_number": gr_num, "po_number": po_num,
        "vendor_name": vendor["name"], "gr_date": str(gr_date),
        "gr_status": gr_status, "line_items": received
    }

# ── Invoice PDF ───────────────────────────────────────────────────────────────
def gen_invoice(inv_num, po_num, vendor, line_items, inv_date, due_date,
                payment_status, path,
                qty_override=None, price_override=None):
    """
    qty_override:   dict of {material_id: qty}   — overrides invoice qty per line
    price_override: dict of {material_id: price} — overrides unit price per line
    """
    rows = [["#", "Description", "Unit", "Qty",
             "Unit Price\n(AED)", "Amount\n(AED)"]]
    subtotal = 0
    inv_lines = []
    for i, li in enumerate(line_items, 1):
        m    = li["material"]
        qty  = (qty_override or {}).get(m["material_id"], li["qty"])
        uprice = (price_override or {}).get(m["material_id"], m["unit_price"])
        amt  = round(uprice * qty, 2)
        subtotal += amt
        rows.append([str(i), m["name"], m["unit"], str(qty),
                     f"{uprice:,.2f}", f"{amt:,.2f}"])
        inv_lines.append({"material_id": m["material_id"],
                          "material_name": m["name"],
                          "qty": qty, "unit_price": uprice,
                          "line_amount": amt})

    subtotal = round(subtotal, 2)
    vat      = round(subtotal * 0.05, 2)
    total    = round(subtotal + vat, 2)

    col_w = [8, 68, 12, 12, 26, 26]
    col_w = [c * mm for c in col_w]

    story = [
        Paragraph("TAX INVOICE", H1),
        Paragraph(f"{vendor['name']}  |  {vendor['address']}  |  TRN: {vendor['trn']}", SM),
        Spacer(1, 3*mm), divider(), Spacer(1, 3*mm),
        header_block(
            [("Invoice No:",   inv_num),
             ("Invoice Date:", str(inv_date)),
             ("Due Date:",     str(due_date)),
             ("PO Reference:", po_num),
             ("Payment:",      payment_status)],
            [("Bill To:",      BUYER["name"]),
             ("Address:",      BUYER["address"]),
             ("TRN:",          BUYER["trn"])]
        ),
        Spacer(1, 5*mm),
        Paragraph("Invoice Items", H2),
        Table(rows, colWidths=col_w, style=tbl_style()),
        Spacer(1, 3*mm),
        totals_block(subtotal, vat, total),
        Spacer(1, 5*mm),
        Paragraph(f"Bank: {BUYER['bank']}  |  IBAN: {BUYER['iban']}  |  Swift: {BUYER['swift']}", SM),
        Spacer(1, 3*mm),
        Paragraph("This is a computer-generated tax invoice for portfolio/demo purposes only.", CTR),
    ]
    build(path, story)

    return {
        "invoice_number": inv_num, "po_number": po_num,
        "vendor_name": vendor["name"], "invoice_date": str(inv_date),
        "due_date": str(due_date), "payment_status": payment_status,
        "line_items": inv_lines, "total_aed": total
    }

# ── Date helpers ──────────────────────────────────────────────────────────────
def make_dates(offset=0):
    po_d  = date(2025, 1, 1) + timedelta(days=offset)
    del_d = po_d  + timedelta(days=14)
    gr_d  = po_d  + timedelta(days=10)
    inv_d = po_d  + timedelta(days=12)
    due_d = inv_d + timedelta(days=30)
    return po_d, del_d, gr_d, inv_d, due_d

# ── Ground truth tracking ─────────────────────────────────────────────────────
ground_truth = []
counter = [0]

def next_ids():
    counter[0] += 1
    n = counter[0]
    return f"PO-2025-{n:04d}", f"GR-2025-{n:04d}", f"INV-2025-{n:04d}"

def record(scenario, po_num, inv_num, gr_num,
           overall, po_match, gr_match, notes):
    ground_truth.append({
        "scenario":              scenario,
        "po_number":             po_num,
        "invoice_number":        inv_num,
        "gr_number":             gr_num,
        "expected_overall_match":overall,
        "expected_po_match":     po_match,
        "expected_gr_match":     gr_match,
        "notes":                 notes,
    })

# ══════════════════════════════════════════════════════════════════════════════
# SCENARIO GENERATION
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. MATCH_EXACT (15) ───────────────────────────────────────────────────────
print("Generating MATCH_EXACT ...")
for i in range(15):
    v  = random.choice(VENDORS)
    lines = pick_line_items()               # 1–4 random line items
    po_num, gr_num, inv_num = next_ids()
    po_d, del_d, gr_d, inv_d, due_d = make_dates(i * 5)

    gen_po(po_num, v, lines, po_d, del_d, f"{PO_DIR}/{po_num}.pdf")
    gen_gr(gr_num, po_num, v, lines, gr_d, "RECEIVED", f"{GR_DIR}/{gr_num}.pdf")
    gen_invoice(inv_num, po_num, v, lines, inv_d, due_d,
                "PENDING", f"{IV_DIR}/{inv_num}.pdf")
    n_lines = len(lines)
    record("MATCH_EXACT", po_num, inv_num, gr_num, True, True, True,
           f"{n_lines} line item(s) — all fields match exactly")

# ── 2. MATCH_TOLERANCE ±2–4% (10) ────────────────────────────────────────────
print("Generating MATCH_TOLERANCE ...")
for i in range(10):
    v  = random.choice(VENDORS)
    lines = pick_line_items()
    po_num, gr_num, inv_num = next_ids()
    po_d, del_d, gr_d, inv_d, due_d = make_dates(80 + i * 4)

    pct = random.uniform(0.02, 0.04)
    # Apply tolerance to every line item
    price_ov = {li["material"]["material_id"]: round(li["material"]["unit_price"]*(1+pct),2)
                for li in lines}

    gen_po(po_num, v, lines, po_d, del_d, f"{PO_DIR}/{po_num}.pdf")
    gen_gr(gr_num, po_num, v, lines, gr_d, "RECEIVED", f"{GR_DIR}/{gr_num}.pdf")
    gen_invoice(inv_num, po_num, v, lines, inv_d, due_d, "PENDING",
                f"{IV_DIR}/{inv_num}.pdf", price_override=price_ov)
    record("MATCH_TOLERANCE", po_num, inv_num, gr_num, True, True, True,
           f"{len(lines)} line(s) — price +{pct*100:.1f}% over PO (within 5% tolerance)")

# ── 3. MATCH_PARTIAL_DELIVERY (8) ────────────────────────────────────────────
print("Generating MATCH_PARTIAL_DELIVERY ...")
for i in range(8):
    v  = random.choice(VENDORS)
    lines = pick_line_items()
    po_num, gr_num, inv_num = next_ids()
    po_d, del_d, gr_d, inv_d, due_d = make_dates(125 + i * 5)

    # Each line is partially received (50–80% of PO qty)
    rec_ov = {}
    qty_ov = {}
    for li in lines:
        mid = li["material"]["material_id"]
        partial = max(1, int(li["qty"] * random.uniform(0.50, 0.80)))
        rec_ov[mid] = partial
        qty_ov[mid] = partial  # invoice qty matches received qty

    gen_po(po_num, v, lines, po_d, del_d, f"{PO_DIR}/{po_num}.pdf")
    gen_gr(gr_num, po_num, v, lines, gr_d, "PARTIAL",
           f"{GR_DIR}/{gr_num}.pdf", received_qty_override=rec_ov)
    gen_invoice(inv_num, po_num, v, lines, inv_d, due_d, "PENDING",
                f"{IV_DIR}/{inv_num}.pdf", qty_override=qty_ov)
    record("MATCH_PARTIAL_DELIVERY", po_num, inv_num, gr_num, True, True, True,
           f"{len(lines)} line(s) — partial delivery, invoice matches GR qty")

# ── 4. EXC_QTY_MISMATCH (8) ──────────────────────────────────────────────────
print("Generating EXC_QTY_MISMATCH ...")
for i in range(8):
    v  = random.choice(VENDORS)
    lines = pick_line_items()
    po_num, gr_num, inv_num = next_ids()
    po_d, del_d, gr_d, inv_d, due_d = make_dates(168 + i * 4)

    # Invoice overstates qty on at least one (random) line
    offending = random.choice(lines)
    mid = offending["material"]["material_id"]
    qty_ov = {mid: offending["qty"] + random.randint(3, 12)}

    gen_po(po_num, v, lines, po_d, del_d, f"{PO_DIR}/{po_num}.pdf")
    gen_gr(gr_num, po_num, v, lines, gr_d, "RECEIVED", f"{GR_DIR}/{gr_num}.pdf")
    gen_invoice(inv_num, po_num, v, lines, inv_d, due_d, "PENDING",
                f"{IV_DIR}/{inv_num}.pdf", qty_override=qty_ov)
    record("EXC_QTY_MISMATCH", po_num, inv_num, gr_num, False, True, False,
           f"Line {mid}: invoice qty={qty_ov[mid]} > GR qty={offending['qty']}")

# ── 5. EXC_PRICE_MISMATCH >5% (6) ────────────────────────────────────────────
print("Generating EXC_PRICE_MISMATCH ...")
for i in range(6):
    v  = random.choice(VENDORS)
    lines = pick_line_items()
    po_num, gr_num, inv_num = next_ids()
    po_d, del_d, gr_d, inv_d, due_d = make_dates(204 + i * 5)

    offending = random.choice(lines)
    mid   = offending["material"]["material_id"]
    over_pct = random.uniform(0.08, 0.22)
    price_ov = {mid: round(offending["material"]["unit_price"] * (1 + over_pct), 2)}

    gen_po(po_num, v, lines, po_d, del_d, f"{PO_DIR}/{po_num}.pdf")
    gen_gr(gr_num, po_num, v, lines, gr_d, "RECEIVED", f"{GR_DIR}/{gr_num}.pdf")
    gen_invoice(inv_num, po_num, v, lines, inv_d, due_d, "PENDING",
                f"{IV_DIR}/{inv_num}.pdf", price_override=price_ov)
    record("EXC_PRICE_MISMATCH", po_num, inv_num, gr_num, False, False, True,
           f"Line {mid}: invoice price +{over_pct*100:.1f}% over PO — exceeds 5%")

# ── 6. EXC_WRONG_VENDOR (5) ──────────────────────────────────────────────────
print("Generating EXC_WRONG_VENDOR ...")
for i in range(5):
    v_po  = VENDORS[i % len(VENDORS)]
    v_inv = VENDORS[(i + 3) % len(VENDORS)]
    lines = pick_line_items()
    po_num, gr_num, inv_num = next_ids()
    po_d, del_d, gr_d, inv_d, due_d = make_dates(238 + i * 5)

    gen_po(po_num, v_po,  lines, po_d, del_d, f"{PO_DIR}/{po_num}.pdf")
    gen_gr(gr_num, po_num, v_po, lines, gr_d, "RECEIVED", f"{GR_DIR}/{gr_num}.pdf")
    gen_invoice(inv_num, po_num, v_inv, lines, inv_d, due_d, "PENDING",
                f"{IV_DIR}/{inv_num}.pdf")
    record("EXC_WRONG_VENDOR", po_num, inv_num, gr_num, False, False, False,
           f"PO vendor={v_po['name']} ≠ Invoice vendor={v_inv['name']}")

# ── 7. EXC_DUPLICATE (4 pairs = 8 invoices) ──────────────────────────────────
print("Generating EXC_DUPLICATE ...")
for i in range(4):
    v  = random.choice(VENDORS)
    lines = pick_line_items()
    po_num, gr_num, inv_num = next_ids()
    po_d, del_d, gr_d, inv_d, due_d = make_dates(265 + i * 5)
    dup_num = inv_num + "-DUP"

    gen_po(po_num, v, lines, po_d, del_d, f"{PO_DIR}/{po_num}.pdf")
    gen_gr(gr_num, po_num, v, lines, gr_d, "RECEIVED", f"{GR_DIR}/{gr_num}.pdf")
    gen_invoice(inv_num, po_num, v, lines, inv_d, due_d, "PENDING",
                f"{IV_DIR}/{inv_num}.pdf")
    gen_invoice(dup_num, po_num, v, lines, inv_d, due_d, "PENDING",
                f"{IV_DIR}/{dup_num}.pdf")
    record("EXC_DUPLICATE_ORIGINAL",  po_num, inv_num, gr_num,
           True,  True, True,  "Original — first submission")
    record("EXC_DUPLICATE_RESUBMIT",  po_num, dup_num, gr_num,
           False, True, True,  "Duplicate — same PO/GR already matched")

# ── 8. EXC_GR_DATE_AFTER_INVOICE (4) ─────────────────────────────────────────
print("Generating EXC_GR_DATE_AFTER_INVOICE ...")
for i in range(4):
    v  = random.choice(VENDORS)
    lines = pick_line_items()
    po_num, gr_num, inv_num = next_ids()
    po_d, del_d, _, _, _ = make_dates(288 + i * 5)
    inv_d_early = po_d + timedelta(days=3)   # invoice BEFORE goods arrive
    gr_d_late   = po_d + timedelta(days=15)  # goods arrive AFTER invoice
    due_d       = inv_d_early + timedelta(days=30)

    gen_po(po_num, v, lines, po_d, del_d, f"{PO_DIR}/{po_num}.pdf")
    gen_gr(gr_num, po_num, v, lines, gr_d_late, "RECEIVED", f"{GR_DIR}/{gr_num}.pdf")
    gen_invoice(inv_num, po_num, v, lines, inv_d_early, due_d, "PENDING",
                f"{IV_DIR}/{inv_num}.pdf")
    record("EXC_GR_DATE_AFTER_INVOICE", po_num, inv_num, gr_num, False, True, False,
           f"Invoice {inv_d_early} before GR {gr_d_late} — fraud signal")

# ── Write ground truth CSV ─────────────────────────────────────────────────────
gt_path = f"{BASE}/ground_truth.csv"
with open(gt_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=ground_truth[0].keys())
    writer.writeheader()
    writer.writerows(ground_truth)

# ── Summary ───────────────────────────────────────────────────────────────────
po_cnt  = len(os.listdir(PO_DIR))
gr_cnt  = len(os.listdir(GR_DIR))
inv_cnt = len(os.listdir(IV_DIR))

print(f"""
Done!
───────────────────────────────────────────────
  POs generated:      {po_cnt:>3}  →  {PO_DIR}/
  GRs generated:      {gr_cnt:>3}  →  {GR_DIR}/
  Invoices generated: {inv_cnt:>3}  →  {IV_DIR}/
  Ground truth rows:  {len(ground_truth):>3}  →  {gt_path}
───────────────────────────────────────────────
  Scenarios:
    MATCH_EXACT                :  15
    MATCH_TOLERANCE            :  10  (price +2–4%, within 5%)
    MATCH_PARTIAL_DELIVERY     :   8  (50–80% of lines received)
    EXC_QTY_MISMATCH           :   8  (invoice qty > GR qty)
    EXC_PRICE_MISMATCH         :   6  (price >5% over PO)
    EXC_WRONG_VENDOR           :   5  (vendor mismatch)
    EXC_DUPLICATE              :   4  (8 invoices: orig + dup)
    EXC_GR_DATE_AFTER_INVOICE  :   4  (fraud: invoice before GR)
───────────────────────────────────────────────
  All vendors: 100% fictional names
  Line items:  1–4 per document (weighted random)
""")
