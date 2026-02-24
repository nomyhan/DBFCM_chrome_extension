# Dog's Best Friend - Scheduling Quick Reference

## Hours & Days
**Open:** Tuesday - Saturday, 8:30 AM - 5:30 PM
**Closed:** Sunday, Monday, Federal Holidays

## Time Slots
| Slot | Notes |
|------|-------|
| 8:30 | Full service, handstrip. Bath-only OK if Elmer has gap (see below) |
| 10:00 | Full service |
| 11:30 | Full service |
| 1:30 | Full service or bath-only |
| 2:30 | Full service or bath-only. NOT on online booking — reserve slot based on workload |

**Bath-Only Timing:**
- **Preferred:** 1:30 or 2:30 to avoid interfering with groomer baths
- **8:30 exception:** Works when Tomoko has SM/XS dog, Kumi has handstrip (baths herself), and Mandilyn does her own first dog — Elmer is idle ~9:00-10:00
- **Goal:** All dogs in by 2:30. Exceptions happen but that's the target.

## Service Types
| Service | System Flags |
|---------|--------------|
| Bath Only | GLBath=-1, GLGroom=0 |
| Full Service | GLBath=-1, GLGroom=-1 |
| Handstrip | GLOthersID assigned |

## Duration & Overlap Logic

**Calendar shows 90 min for all full service** — this is intentional to keep next slot open for online booking.

**Client messaging:** We tell clients to allow up to 4 hours, though usually less.

**Actual Durations:**
| Size | Bath | Groom | Total |
|------|------|-------|-------|
| XS/SM | 30-45 min | 75-90 min | ~2 hr |
| MD/LG | 45-60 min | 90-120 min | 2.5-3 hr |
| XL | Varies | Varies | Case by case |
| Fancy grooms (e.g., Penelope) | 2 hr | 2 hr | 4 hr |

**Why this works:** Elmer completes the bath before the groomer finishes the previous groom, so they're ready for the next haircut.

**Exceptions:**
- **Handstrip:** Kumi does entire appointment herself — no overlap possible with her next handstrip, but CAN overlap into a normal full service
- **Matting:** Extends time. We charge $3/min after first 10 min. Many clients opt for short shave instead.
- **Multiple MD/LG/XL at same slot:** May overload Elmer — use judgment
- **Multiple dogs, same client, same groomer:** Add time accordingly — overlap logic doesn't apply

## Groomers - Who Does What

| Groomer | Full Service | Handstrip | Show Dogs | Notes |
|---------|:------------:|:---------:|:---------:|-------|
| **Tomoko** (85) | ✓ | ✗ | ✓ | Any breed, but prefers not LG/XL |
| **Kumi** (59) | ✓ | ✓ | ✓ | Any breed. ONLY handstrip groomer. Prefers not LG/XL |
| **Mandilyn** (95) | ✓ | ✗ | Some | Default for new clients, no-preference, and LG/XL dogs |

**Booking priority:**
- Client has a preference → book their preferred groomer
- LG/XL dogs (German Shepherds, Labs, Goldendoodles, etc.) → Mandilyn
- New client or no preference → Mandilyn first
- Handstrip breed (#) → Kumi only

**New LG/XL dogs:** Clear with groomer before booking. Some breeds (e.g., Samoyeds) may be declined.

**Online booking without groomer selected:**
- "Mandilyn dog" lands on Tomoko/Kumi → usually accommodate
- Specialty dog lands on Mandilyn (e.g., Bedlington) → reject and reschedule with correct groomer

## Bathers
| Bather | Role |
|--------|------|
| **Elmer** (8) | Primary bather - handles most baths, can cycle multiple dogs |
| **Guest Groomer** (97) | Placeholder for substitute when Elmer is out |

**Bather Logic:** Elmer can have overlapping bath assignments - he cycles through dogs.

**Capacity check:** If 2+ dogs arrive at the same time for different groomers, Elmer may not finish in time. Groomers will have to wait, throwing off the schedule. Consider this when booking.

**When Elmer is out:** Either use a substitute (Guest Groomer) or reduce workload and have groomers bathe their own dogs.

## Pet Name Markers
| Symbol | Meaning | Action |
|--------|---------|--------|
| # | Handstrip breed | MUST book with Kumi |
| * | Show dog | Tomoko or Kumi preferred |

## Multi-Pet Households
- **No preference:** Book same slot with different groomers (parallel)
- **Has preference or only one groomer available:** Book both with same groomer, span 2 slots (180 min instead of 90) — verify both slots are available
- **Watch for online booking errors:** Clients often book 2 dogs in one slot without realizing only one slot is actually open

## Client Preferences
Before booking, check:
1. **Pet notes (PtGroom)** - grooming time, special instructions
2. **Client warnings (CLWarning)** - payment issues, special needs
3. **History** - preferred day of week, usual groomer

## Blocked Time
Check for vacations, dog shows (especially Kumi), personal days.

**How to check:**
- **Live Access GUI:** Must select specific groomer by name to see their blocked time in calendar view
- **Easier option:** Check the physical paper calendar at the desk

## When to Escalate
- New client (needs full intake)
- Pricing questions
- Complaints or unhappy clients
- Requests for specific dates when fully booked
- Anything involving payments or refunds
- Client wants groomer who doesn't match their pet's needs

## Quick Availability Check
1. What service? (Bath only / Full service / Handstrip)
2. Which groomers can do it?
3. Check those groomers' schedules for open slots
4. Check Elmer's availability for bath portion
5. Confirm no BlockedTime conflicts

## Holidays (Closed)
Most federal holidays fall on Monday (already closed). Watch for these:
- New Year's Day (Jan 1)
- Independence Day (July 4)
- Thanksgiving (Thursday)
- Christmas (Dec 25)

## Online Booking Workarounds

**Note:** Out-times are internal only. Clients only see in-time (drop-off time). These notes are for staff.

**Online slots available:** 8:30, 10:00, 11:30, 1:30 only (2:30 is reserve, not shown)

**The 90-min fiction:** Online availability assumes Elmer is bathing. The 8:30 appointment shows out-time as 10:00 so the 10:00 slot appears open. In reality:
- 8:30 dog arrives, Elmer baths, groomer starts groom
- 10:00 dog arrives, Elmer starts bath
- Groomer finishes 8:30 groom around 11:00, starts 10:00 groom
- And so on...

**Rules:**
- Never change in-times without confirming with customer
- Can manipulate out-times to show or block availability

**Off-time appointments (e.g., 9:00 or 10:30):**
- Problem: A 9:00 booking makes 8:30 look available online, but groomer is busy
- Solution A: Add a dummy "Do Not Book" appointment before it to hide the slot
- Solution B: Extend the previous appointment's out-time (e.g., make 8:30 appointment 120 min instead of 90 to block 10:00 if booking at 10:30)

**If groomers are bathing their own dogs:** Overlap doesn't work — adjust out-times to reflect actual duration.
