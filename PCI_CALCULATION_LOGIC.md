# PCI Road Condition Calculation Logic

**Standard:** ASTM D6433 — Standard Practice for Roads and Parking Lots Pavement Condition Index Surveys
**File:** `processing_worker/pci_calculator.py`

---

## What is PCI?

**PCI (Pavement Condition Index)** is a numerical score from **0 to 100** that describes the overall condition of a road surface.

```
PCI = 100 - CDV
```

| PCI Score | Condition Rating |
|-----------|-----------------|
| 85 – 100  | Excellent        |
| 70 – 85   | Very Good        |
| 55 – 70   | Good             |
| 40 – 55   | Fair             |
| 25 – 40   | Poor             |
| 10 – 25   | Very Poor        |
| 0 – 10    | Failed           |

A perfect road scores 100. A completely destroyed road scores 0.

---

## Step-by-Step: How We Calculate PCI

The calculation has **4 stages**:

```
Violations  →  Density  →  Deduct Value (DV)  →  CDV  →  PCI
```

---

### Stage 1 — What is Density?

**Density** answers the question: *"What percentage of the road surface is affected by this type of damage?"*

```
Density (%) = (Distress Area / Total Pavement Area) × 100
```

**Distress Area** = total polygon area of all detected violations of the same type and severity.
For example: sum of all polygon areas of "pothole + high severity" violations in one segment.

**Pavement Area** = the total road surface area that was inspected:

- If GSD (Ground Sampling Distance) is available:
  ```
  Pavement Area = image_width × image_height × (gsd_mm_per_px)²   [in mm²]
  ```

- If GSD is not available (our current model does not output GSD):
  ```
  Pavement Area = image_width × image_height   [in pixels²]
  ```
  In this fallback case, `polygon_area_mm2` is also in pixel units, so the ratio
  (density) stays dimensionally consistent.

**For a segment**, the total pavement area is the sum of individual frame areas across
all unique frames that have violations in that segment:
```
Total Pavement Area = Σ (frame area per unique frame_id)
```

#### Example

A 1280×720 frame has 3 longitudinal cracks each with a polygon area of ~2000 px²:

```
Distress Area  = 3 × 2000 = 6000 px²
Pavement Area  = 1280 × 720 = 921,600 px²
Density        = (6000 / 921600) × 100 = 0.65%
```

---

### Stage 2 — Deduct Value (DV)

A **Deduct Value (DV)** is a penalty score (0–100) for a specific distress type at a specific density.

Each combination of `(distress_type, severity)` has its own DV curve. Higher density = higher DV = worse condition.

#### Distress Types We Detect

| Model Label         | ASTM Distress Type          |
|---------------------|-----------------------------|
| `pothole`           | Potholes                    |
| `alligator_crack`   | Alligator (Fatigue) Cracking|
| `longitudinal_crack`| Longitudinal Cracking       |
| `transverse_crack`  | Transverse Cracking         |
| `rutting`           | Rutting                     |
| `road_crack`        | → mapped to Longitudinal    |

#### DV Table (examples)

**Potholes — High Severity:**
```
Density 0.01% → DV 15
Density 0.10% → DV 35
Density 0.50% → DV 55
Density 1.00% → DV 65
Density 5.00% → DV 80
```

**Alligator Crack — Medium Severity:**
```
Density  0.1% → DV  9
Density  1.0% → DV 21
Density  5.0% → DV 38
Density 10.0% → DV 48
Density 20.0% → DV 56
```

Values between breakpoints are computed using **piecewise linear interpolation**
(straight line between the two nearest known points).

Continuing the example from Stage 1 (density = 0.65%, longitudinal crack, low severity):
```
DV table for (longitudinal_crack, low):  0.1→2,  1→7
Interpolate at 0.65:  DV = 2 + ((0.65 - 0.1) / (1 - 0.1)) × (7 - 2)
                          = 2 + 0.61 × 5 = 5.06
```

---

### Stage 3 — Corrected Deduct Value (CDV)

The problem with simply summing all DVs is that it **over-penalizes** a road that has many small
problems vs. one big problem. ASTM D6433 corrects for this with an iterative method.

#### Inputs
- All individual DVs (one per distress type + severity combination)
- A correction table indexed by `q` (number of active deduct values > 2.0)

#### Iterative Algorithm

```
1. Sort all DVs descending.
   Example: [44, 22, 8, 3]

2. q = count of DVs > 2.0 = 4
   TDV (Total Deduct Value) = 44 + 22 + 8 + 3 = 77

3. Look up CDV from the correction table for (TDV=77, q=4)
   → CDV ≈ 29.8   ← save this

4. Replace the smallest DV that is still > 2.0 with 2.0:
   Working list: [44, 22, 8, 2.0]

5. q = 3, TDV = 44 + 22 + 8 + 2 = 76
   Look up CDV for (TDV=76, q=3) → CDV ≈ 36.0  ← save if larger

6. Replace again: [44, 22, 2.0, 2.0]

7. q = 2, TDV = 70
   Look up CDV for (TDV=70, q=2) → CDV ≈ 40.0  ← save if larger

8. Replace again: [44, 2.0, 2.0, 2.0]

9. q = 1, TDV = 50
   Look up CDV for (TDV=50, q=1) → CDV ≈ 38.0

10. q == 1 → STOP. Final CDV = max across all iterations = 40.0
```

**Why does q decrease matters?**
The correction table gives a lower CDV as q decreases — it acknowledges that many small
distresses together do not degrade the road as much as a single large one of the same total
deduct value.

---

### Stage 4 — Final PCI

```
PCI = 100 - CDV
```

Continuing the example:
```
CDV = 40.0
PCI = 100 - 40 = 60  →  "Good"
```

---

## Frame vs. Segment Level

### Frame PCI
Computed for **one camera image** immediately after the model processes it.

```
frame_pci = compute_frame_pci(violations, image_width, image_height, gsd)
```

- Pavement area = area of that single frame
- Violations = only detections from that frame

### Segment PCI
Computed for an **entire road segment** (a continuous stretch of road covered by multiple frames).

```
segment_pci = compute_segment_pci(all_violations_in_segment)
```

- Pavement area = sum of all unique frame areas in the segment
- Violations = all detections across every frame in the segment
- **Recomputed every time a new frame is processed** for that segment so the score stays current

This means a segment's PCI improves or worsens dynamically as more frames are ingested.

---

## Where PCI is Stored and Displayed

| Location         | Column        | Updated When                          |
|------------------|---------------|---------------------------------------|
| `frames` table   | `pci_score`, `pci_rating` | Immediately after each frame is processed |
| `segments` table | `pci_score`, `pci_rating` | After each frame that belongs to it   |
| `/api/segments`  | Returned in JSON | On every map refresh (every 10s)   |
| Map segment lines| Color-coded   | Green (Excellent) → Red (Failed)      |
| Segment panel    | PCI badge     | Shown when you click a segment        |

### Segment Color Scale on Map

| Score Range | Color  | Rating    |
|-------------|--------|-----------|
| 85 – 100    | Green  | Excellent |
| 70 – 85     | Light Green | Very Good |
| 55 – 70     | Yellow-Green | Good  |
| 40 – 55     | Yellow | Fair      |
| 25 – 40     | Orange | Poor      |
| 10 – 25     | Red    | Very Poor |
| 0 – 10      | Dark Red | Failed  |

---

## Current Results (Riyadh Dataset — 81 frames processed)

| Rating     | Segments | Avg Score |
|------------|----------|-----------|
| Excellent  | 177      | 96.1      |
| Very Good  | 8        | 80.9      |
| Good       | 4        | 65.8      |

Most segments score high (Excellent) because the violations detected are small longitudinal
cracks covering less than 1% of the frame area each. As more frames are uploaded
and larger/denser defects are detected, scores will lower.
