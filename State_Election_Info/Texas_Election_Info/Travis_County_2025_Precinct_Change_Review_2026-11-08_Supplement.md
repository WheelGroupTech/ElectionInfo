# Travis County 2025 Precinct Changes: Correction Supplement

**Companion to** `Travis_County_2025_Precinct_Change_Review_2026-11-08.md`  
**For:** Travis County Tax Office, Voter Registration Division, and the Travis County Clerk, Elections Division  
**Date:** November 8, 2026  
**Prepared by:** Daniel M. Teal, dan@teals.org

This supplement itemises the data errors found in the review so that each can be corrected. Every finding is derived from Travis County's own published data: the precinct shapefile adopted by the Commissioners Court on December 4, 2025, the ArcGIS services at `taxmaps.traviscountytx.gov` (`Precincts`, `AdMas`, `VotingAddresses`, `UnregisteredAddresses`, `Splits`, `Parcels`), the county's published voter registration lists, and the TEAM Official Lists of Registered Voters the county provided.

Voters are identified by VUID only. No voter is named and no voter is linked here to a street address. The street addresses listed are property-level facts from the county's own address master, appraisal parcel records and adopted maps.

The machine-readable form of every item is in `Travis_County_2025_Precinct_Change_Review_2026-11-08_Records.csv`, with columns `section`, `category`, `key` and `detail`. Section letters below map to the record sections as follows: A → S3, B → S1, C → S4, D → S5 and S6, E → S8, F → S7, G → S9, H → S10, I → S11 and S12, J → S2.

## How each finding was established

Three sources independently determine the precinct an address belongs to:

1. the **adopted precinct polygons** (the Commissioners Court order of December 4, 2025, effective January 1, 2026);
2. the county's **address master** (`AdMas`), which assigns a precinct to each of 412,053 addresses;
3. the **registration lists** — the county Voter List and the TEAM OLRV.

Sources 1 and 2 agree on 411,674 of 412,053 address points (99.91%), and on every one of the 96 resolvable addresses in section A; the 97th has no usable point geometry and is left unresolved. Where the registration list differs from both, the list is treated as the error. Where sources 1 and 2 differ from each other, the address is listed separately in section D.

## Priority order

| Priority | Item | Count | Section |
|---|---|---:|---|
| 1 | Voters who cast a G26 ballot from a precinct with the wrong districts | 71 voters | B |
| 2 | Invalid precinct values published for precinct 369 (the roster was correct; the two extracts were not) | 9 voters | G |
| 3 | Addresses coded to the wrong precinct on the Voter List | 97 addresses | A |
| 4 | Address district codes conflicting with their precinct (§42.005) | 20 addresses | C |
| 5 | Address master points coded to a precinct they are not in | 28 addresses | D |
| 6 | TEAM OLRV precinct disagreements to confirm | 476 addresses | J |
| 7 | Stale 2021 split numbering in `AdMas` and the `Splits` layer | 32,067 points, 1,133 polygons | E |
| 8 | Retired precinct numbers in `UnregisteredAddresses` | 5,203 points | F |
| 9 | Precinct "99999" registrations | 14 voters | H |
| 10 | Precincts outside the §42.006 size range, and empty precincts | 23 + 9 precincts | I |

## A. Addresses coded to the wrong precinct on the October 20, 2026 Voter List

97 addresses. `List` is the precinct on the county Voter List; `Correct` is what both the adopted precinct map and the county address master say. `Districts wrong` lists the district types that differ between the two precincts.

| # | Address | ZIP | Voters | List | Correct | Districts wrong |
|---:|---|---|---:|---|---|---|
| 1 | 8000 US 290 | 78736 | 11 | 131 | 357 | House, Comm, JP |
| 2 | 7880 US 290 | 78736 | 8 | 131 | 357 | House, Comm, JP |
| 3 | 3501 FM 620 | 78738 | 8 | 370 | 292 | Comm, JP |
| 4 | 1007 CONGRESS | 78704 | 4 | 300 | 411 | Cong, House, Comm, JP |
| 5 | 10705 WATERING ROCK LN | 78759 | 2 | 182 | 369 | Cong, House, Comm, JP |
| 6 | 5302 HANGING CLIFF CV | 78759 | 2 | 182 | 369 | Cong, House, Comm, JP |
| 7 | 1024 LUCINDA WILLIAMS WAY | 78744 | 2 | 231 | 464 | Cong, Sen, House, Comm, JP |
| 8 | 5 LOST MEADOW CV | 78738 | 2 | 376 | 374;376 | JP |
| 9 | 309 E 11TH ST | 78701 | 2 | 410;430 | 300 | Cong, House, Comm, JP |
| 10 | 2303 HARWICK DR | 78745 | 2 | 439 | 331 | Sen, House, Comm |
| 11 | 7631 E US 290 HWY | 78736 | 1 | 121 | 357 | House, Comm, JP |
| 12 | 5308 HANGING CLIFF | 78759 | 1 | 182 | 369 | Cong, House, Comm, JP |
| 13 | 813 CAPITOL CT | 78756 | 1 | 104 | 212 | House, Comm, JP |
| 14 | 3313 ZACH SCOTT ST | 78723 | 1 | 107 | 106 | none |
| 15 | 3025 SUNRIDGE DR | 78741 | 1 | 108 | 426 | Sen, House, Comm, JP |
| 16 | 12062 LINCOLNSHIRE | 78758 | 1 | 114 | 246 | House, Comm, JP |
| 17 | 6107 DECKER LN | 78724 | 1 | 120 | 123 | House |
| 18 | 6816 BOYCE LN | 78653 | 1 | 121 | 141 | none |
| 19 | 15433 FM 1325 | 78728 | 1 | 121 | 193 | Cong, House, JP |
| 20 | 12100 SCARLET SLEEVE WAY | 78617 | 1 | 126 | 483 | Cong, Sen, House, Comm, JP |
| 21 | 2028 E BEN WHITE BLVD | 78741 | 1 | 126 | 425 | Sen, House, Comm, JP |
| 22 | 21008 WINDMILL RIDGE ST | 78660 | 1 | 126 | 155 | Cong, JP |
| 23 | 4305 TEXOMA DR | 78660 | 1 | 127 | 150 | Cong, JP |
| 24 | 20409 TUBERVILLE LN | 78653 | 1 | 127 | 145 | Cong |
| 25 | 12101 DESSAU RD | 78754 | 1 | 132 | 138 | none |
| 26 | 10205 IH 35 | 78728 | 1 | 135 |  (map: (no geometry)) | none |
| 27 | 1103 SOMERSET AVE | 78753 | 1 | 138 | 135 | JP |
| 28 | 8703 SUPONA ST | 78724 | 1 | 138 | 120 | House |
| 29 | 2937 FIRE ROCK DR | 78641 | 1 | 162 | 388 | Sen, House, Comm |
| 30 | 14100 JOHN HENRY FAULK DR | 78660 | 1 | 162 | 182 | Cong, House, JP |
| 31 | 3501 SHORELINE DR | 78728 | 1 | 171 | 193 | House |
| 32 | 10701 WATERING ROCK LN | 78759 | 1 | 182 | 369 | Cong, House, Comm, JP |
| 33 | 9127 RESEARCH BLVD | 78758 | 1 | 192 | 241 | Cong, House, Comm |
| 34 | 14300 TANDEM BLVD | 78728 | 1 | 193 | 196 | Cong |
| 35 | 3220 DUVAL RD | 78759 | 1 | 203 | 276 | House, JP |
| 36 | 1112 NORTH | 78756 | 1 | 213 | 212 | none |
| 37 | 7400 N LAMAR BLVD | 78752 | 1 | 214 | 225 | JP |
| 38 | 1515 BRENTWOOD ST | 78757 | 1 | 220 | 221 | none |
| 39 | 1809 WHITNEY WAY | 78741 | 1 | 221 | 423 | Sen, House, Comm, JP |
| 40 | 4504 BROWN BARK PL | 78727 | 1 | 223 | 264 | House, JP |
| 41 | 2001 JUSTIN | 78757 | 1 | 227 | 221 | none |
| 42 | 3816 S LAMAR BLVD | 78704 | 1 | 234 | 307 | House, Comm, JP |
| 43 | 904 W JOHANNA ST | 78704 | 1 | 234 | 410 | House, Comm, JP |
| 44 | 6607 BRODIE LN | 78745 | 1 | 240 | 330 | Comm, JP |
| 45 | 1400 PORTCHESTER CASTLE PATH | 78660 | 1 | 253 | 162 | Cong, House, Comm |
| 46 | 1900 SCOFIELD RIDGE PKWY | 78727 | 1 | 254 | 255 | none |
| 47 | 12208 ANTOINETTE PL | 78727 | 1 | 254 | 264 | House |
| 48 | 5809 RISING HILLS | 78759 | 1 | 261 | 267 | Cong |
| 49 | 4505 DUVAL RD | 78751 | 1 | 261 | 207 | House, JP |
| 50 | 8610 MESA DR | 78759 | 1 | 261 | 237 | House |
| 51 | 11603 NATRONA DR | 78759 | 1 | 262 | 261 | Cong |
| 52 | 14305 TANDEM BLVD | 78728 | 1 | 262 | 196 | House, Comm |
| 53 | 10801 N MOPAC EXPY | 78759 | 1 | 270 | 260 | Cong, House, JP |
| 54 | 2909 PAMELLA CT | 78734 | 1 | 273 | 370 | Comm |
| 55 | 3500 PARMER LN | 78754 | 1 | 276 | 252 | Cong, House, JP |
| 56 | 4001 PALOMA CIR | 78734 | 1 | 276 | 294 | Cong, Sen |
| 57 | 19620 TRANQUILITY FALLS PATH | 78645 | 1 | 280 | 281 | none |
| 58 | 2606 WILSON ST | 78704 | 1 | 280 | 410 | Cong, Sen, House, Comm, JP |
| 59 | 5126 DESTINATION WAY | 78645 | 1 | 280 | 281 | none |
| 60 | 6104 CARRY BACK | 78746 | 1 | 306 | 290 | Cong, House, Comm |
| 61 | 900 S 1ST ST | 78704 | 1 | 307 | 410 | House, Comm, JP |
| 62 | 2207 LEON ST | 78705 | 1 | 311 | 308 | none |
| 63 | 2911 RIO GRANDE ST | 78705 | 1 | 311 | 206 | Comm |
| 64 | 1201 CASTLE HILL ST | 78703 | 1 | 324 | 208 | Cong, House, Comm, JP |
| 65 | 1805 MORTS PL | 78652 | 1 | 328 | 336 | Cong, House |
| 66 | 6004 WHIPPLE WAY | 78745 | 1 | 330 | 331 | none |
| 67 | 1308 RAMBLING ROSE RD | 78745 | 1 | 330 | 447 | Sen, House, Comm |
| 68 | 14204 SUSSMAN CT | 78728 | 1 | 331 | 191 | Cong, House, Comm, JP |
| 69 | 2025 E 7TH ST | 78702 | 1 | 333 | 402 | House, Comm, JP |
| 70 | 13400 BEE CAVE PKWY | 78738 | 1 | 344 | 292 | Cong, Sen, House, Comm |
| 71 | 1304 SWEET BARK ST | 78745 | 1 | 347 | 447 | Sen, House, Comm |
| 72 | 5136 MANSFIELD VIEW CT | 78732 | 1 | 351 | 275 | Sen, Comm, JP |
| 73 | 506 EXPLORER | 78734 | 1 | 370 | 374 | House |
| 74 | 204 ARIA | 78738 | 1 | 371 | 372 | JP |
| 75 | 2201 ARPDALE | 78704 | 1 | 382 | 306 | Cong, House, JP |
| 76 | 1305 ROARING FORK | 78641 | 1 | 388 | 397 | none |
| 77 | 4900 E OLTORF ST | 78741 | 1 | 388 | 427 | Cong, Sen, House, Comm, JP |
| 78 | 1329 MARINA GRAND TER | 78641 | 1 | 390 | 394 | Cong |
| 79 | 401 S CONGRESS AVE | 78701 | 1 | 411 | 301 | Cong, House, Comm, JP |
| 80 | 823 CONGRESS | 78701 | 1 | 411 | 301 | Cong, House, Comm, JP |
| 81 | 912 LIVE OAK | 78704 | 1 | 413 | 410 | none |
| 82 | 2801 CORNISH | 78745 | 1 | 424 | 331 | Sen, House, Comm, JP |
| 83 | 6400 RIVERSIDE | 78741 | 1 | 427 | 424 | none |
| 84 | 5701 S MOPAC EXPY | 78749 | 1 | 427 | 330 | Sen, House, Comm, JP |
| 85 | 7733 BURLESON RD | 78744 | 1 | 427 | 464 | none |
| 86 | 11611 CIRCLE DR | 78748 | 1 | 435 | 337 | Cong, Sen, House, Comm |
| 87 | 10601 MENCHACA RD | 78748 | 1 | 436 | 453 | House |
| 88 | 1712 W 10TH ST | 78703 | 1 | 436 | 200 | Cong, Comm, JP |
| 89 | 5701 1ST | 78745 | 1 | 437 | 438 | Sen, House |
| 90 | 8225 N FM 620 RD | 78726 | 1 | 445 | 381 | Cong, House, Comm, JP |
| 91 | 715 W SLAUGHTER LN | 78748 | 1 | 450 | 454 | none |
| 92 | 13309 CERISE QUEEN WAY | 78610 | 1 | 460 | 475 | Cong |
| 93 | 5314 GOLDEN CANARY LN | 78723 | 1 | 464 | 111 | Sen, House, Comm, JP |
| 94 | 10320 BIG THICKET DR | 78747 | 1 | 471 | 470 | House |
| 95 | 7233 CRESTONE RD | 78744 | 1 | 483 | 464 | Cong |
| 96 | 13332 WINTON DR | 78621 | 1 | 491 | 145 | Cong, Comm |
| 97 | 2320 GRACY FARMS LN | 78758 | 1 | 491 | 244 | House, Comm, JP |

## B. Voters to review: precinct error that changes a district

71 voters who cast a G26 ballot while recorded in a precinct whose districts differ from their address's precinct. Listed by VUID only.

| VUID | Roster precinct (wrong) | Correct precinct | Districts wrong |
|---|---|---|---|
| 2201828123 | P 280 | 410 | US Congress; TX Senate; TX House; Commissioner; JP/Constable |
| 2214679606 | P 445 | 381 | US Congress; TX House; Commissioner; JP/Constable |
| 2195823098 | P 331 | 191 | US Congress; TX House; Commissioner; JP/Constable |
| 2191925566 | P 300 | 411 | US Congress; TX House; Commissioner; JP/Constable |
| 2186559029 | P 411 | 301 | US Congress; TX House; Commissioner; JP/Constable |
| 2171498931 | P 221 | 423 | TX Senate; TX House; Commissioner; JP/Constable |
| 2153337369 | P 300 | 411 | US Congress; TX House; Commissioner; JP/Constable |
| 2141613771 | P 410 | 300 | US Congress; TX House; Commissioner; JP/Constable |
| 1213162262 | P 324 | 208 | US Congress; TX House; Commissioner; JP/Constable |
| 1181243309 | P 427 | 330 | TX Senate; TX House; Commissioner; JP/Constable |
| 1144798787 | P 411 | 301 | US Congress; TX House; Commissioner; JP/Constable |
| 1140047473 | P 435 | 337 | US Congress; TX Senate; TX House; Commissioner |
| 3383098557 | P 121 | 357 | TX House; Commissioner; JP/Constable |
| 3382751470 | P 276 | 252 | US Congress; TX House; JP/Constable |
| 3382672610 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 3382475190 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 2194767204 | P 234 | 410 | TX House; Commissioner; JP/Constable |
| 2190448125 | P 333 | 402 | TX House; Commissioner; JP/Constable |
| 2188765801 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 2173190524 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 2172912719 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 2172507320 | P 192 | 241 | US Congress; TX House; Commissioner |
| 2168523756 | P 104 | 212 | TX House; Commissioner; JP/Constable |
| 2162959881 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 2162940094 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 2162925313 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 2162267562 | P 121 | 357 | TX House; Commissioner; JP/Constable |
| 2156055389 | P 330 | 447 | TX Senate; TX House; Commissioner |
| 2147213523 | P 439 | 331 | TX Senate; TX House; Commissioner |
| 2136689102 | P 121 | 357 | TX House; Commissioner; JP/Constable |
| 2129160890 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 2128122718 | P 307 | 410 | TX House; Commissioner; JP/Constable |
| 2126510521 | P 382 | 306 | US Congress; TX House; JP/Constable |
| 1217235935 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 1212772021 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 1212215192 | P 121 | 357 | TX House; Commissioner; JP/Constable |
| 1212011349 | P 234 | 307 | TX House; Commissioner; JP/Constable |
| 1211963453 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 1189720363 | P 439 | 331 | TX Senate; TX House; Commissioner |
| 1171089446 | P 270 | 260 | US Congress; TX House; JP/Constable |
| 1163875790 | P 121 | 357 | TX House; Commissioner; JP/Constable |
| 1160320989 | P 114 | 246 | TX House; Commissioner; JP/Constable |
| 1140869858 | P 491 | 244 | TX House; Commissioner; JP/Constable |
| 1132244994 | P 131 | 357 | TX House; Commissioner; JP/Constable |
| 1130771073 | P 121 | 193 | US Congress; TX House; JP/Constable |
| 1056377779 | P 121 | 357 | TX House; Commissioner; JP/Constable |
| 2223053302 | P 437 | 438 | TX Senate; TX House |
| 2192991743 | P 370 | 292 | Commissioner; JP/Constable |
| 2167714970 | P 370 | 292 | Commissioner; JP/Constable |
| 2166851518 | P 126 | 155 | US Congress; JP/Constable |
| 2159948956 | P 261 | 207 | TX House; JP/Constable |
| 2151002221 | P 240 | 330 | Commissioner; JP/Constable |
| 1203733928 | P 370 | 292 | Commissioner; JP/Constable |
| 1168777889 | P 127 | 150 | US Congress; JP/Constable |
| 1141951176 | P 276 | 294 | US Congress; TX Senate |
| 2203008899 | P 370 | 374 | TX House |
| 2198755845 | P 311 | 206 | Commissioner |
| 2171879495 | P 193 | 196 | US Congress |
| 2168319781 | P 127 | 145 | US Congress |
| 2159794127 | P 138 | 135 | JP/Constable |
| 2159755694 | P 171 | 193 | TX House |
| 2159505410 | P 460 | 475 | US Congress |
| 2155738973 | P 138 | 120 | TX House |
| 2124119456 | P 254 | 264 | TX House |
| 2120711563 | P 261 | 237 | TX House |
| 1208151472 | P 471 | 470 | TX House |
| 1136106791 | P 262 | 261 | US Congress |
| 1133285901 | P 376 | 374 | JP/Constable |
| 1133272325 | P 376 | 374 | JP/Constable |
| 1131425224 | P 214 | 225 | JP/Constable |
| 1074778554 | P 390 | 394 | US Congress |

## C. Address district codes that conflict with their precinct (TEC 42.005)

| Address | ZIP | Precinct | Field | Address code | Precinct code | Stale split code |
|---|---|---|---|---|---|---|
| 6301 MANOR RD | 78723 | 112 | Legislative | L50 | L46 | 116.01 |
| 6303 MANOR RD | 78723 | 112 | Legislative | L50 | L46 | 116.01 |
| 2906 S FM 973 RD | 78725 | 123 | Commissioner | P04 | P01 | 480.02 |
| 2906 S FM 973 RD | 78725 | 123 | JPConstable | J04 | J01 | 480.02 |
| 2906 S FM 973 RD | 78725 | 123 | Senate | N21 | N14 | 480.02 |
| 10205 N IH 35 | 78753 | 132 | JPConstable | J02 | J01 | (blank) |
| 7300 1/2 S SH 45 W | 78739 | 346 | Congressional | G37 | G27 | (blank) |
| 5505 S SH 45 W | 78739 | 348 | Congressional | G37 | G27 | (blank) |
| 7403 S SH 45 W | 78739 | 348 | Congressional | G37 | G27 | (blank) |
| 7401 S SH 45 W | 78739 | 348 | Congressional | G37 | G27 | (blank) |
| 11800 ARCHELETA BLVD | 78739 | 348 | Congressional | G37 | G27 | (blank) |
| 7401 1/2 S SH 45 W | 78739 | 348 | Congressional | G37 | G27 | (blank) |
| 4016 1/2 GREEN EMERALD TER | 78739 | 349 | Congressional | G37 | G27 | (blank) |
| 11023 S MOPAC EXPY | 78739 | 349 | Congressional | G37 | G27 | (blank) |
| 11301 1/2 S MOPAC EXPY | 78739 | 349 | Congressional | G37 | G27 | (blank) |
| 6526 W US 290 HWY | 78735 | 351 | Congressional | G37 | G10 | 357.02 |
| 6538 W US 290 HWY | 78735 | 351 | Congressional | G37 | G10 | 357.02 |
| 6616 W US 290 HWY | 78735 | 351 | Congressional | G37 | G10 | 357.02 |
| 5 LOST MEADOW CV | 78738 | 374 | JPConstable | J03 | J02 | 376.03 |
| 2727 S LAMAR BLVD | 78704 | 414 | Commissioner | P03 | P04 | 307.01 |

## D. Address master points coded to a precinct their coordinates are not in

| Address | Coded precinct | Precinct the point falls in | Stale split code |
|---|---|---|---|
| 1702 CHICON ST | 100 | 102 | 100.01 |
| 4504 DUVAL ST | 213 | 207 | 213.01 |
| 501 E BRENHAM ST | 127 | 126 | 127.03 |
| 2323 E MARTIN LUTHER KING JR BLVD | 102 | 103 | 103.01 |
| 2383 E MARTIN LUTHER KING JR BLVD | 102 | 103 | 103.01 |
| 2803 E MARTIN LUTHER KING JR BLVD | 102 | 103 | 103.01 |
| 9701 FM 1826 RD | 318 | 360 | 343.04 |
| 9703 FM 1826 RD | 318 | 360 | 343.04 |
| 14495 FM 969 RD | 126 | 491 | 126.04 |
| 10200 GILES LN | 141 | 134 | 141.01 |
| 10300 GILES LN | 141 | 134 | 141.01 |
| 2515 GUADALUPE ST | 312 | 313 | 313.01 |
| 1211 HASKELL ST | 402 | 401 | 401.01 |
| 14305 HUNTERS BEND RD | 126 | 491 | 126.04 |
| 2048 LA CASA DR | 306 | 307 | 306.01 |
| 6301 MANOR RD | 112 | 116 | 116.01 |
| 6303 MANOR RD | 112 | 116 | 116.01 |
| 11301 MC ANGUS RD | 483 | 482 | 482.12 |
| 1402 RABB RD | 307 | 306 | 307.01 |
| 2906 S FM 973 RD | 123 | 480 | 480.02 |
| 2727 S LAMAR BLVD | 414 | 307 | 307.01 |
| 2220 S PLEASANT VALLEY RD | 420 | 427 | 420.01 |
| 15606 TACON LN | 180 | 177 | 180.05 |
| 808 W FM 1626 RD | 337 | 336 | 337.06 |
| 6526 W US 290 HWY | 351 | 357 | 357.02 |
| 6538 W US 290 HWY | 351 | 357 | 357.02 |
| 6616 W US 290 HWY | 351 | 357 | 357.02 |
| 10205 N IH 35 | 132 | 135 | (blank) |

## E. Address master PrecinctSplit values still on 2021 numbering

| Split prefix (2021) | Precinct (2025) | Address points |
|---|---|---:|
| 124.xx | 120 | 2,443 |
| 194.xx | 192 | 2,169 |
| 393.xx | 388 | 1,986 |
| 431.xx | 436 | 1,474 |
| 461.xx | 463 | 1,432 |
| 224.xx | 220 | 1,149 |
| 373.xx | 374 | 1,124 |
| 474.xx | 457 | 1,120 |
| 133.xx | 131 | 1,111 |
| 151.xx | 174 | 1,036 |
| 183.xx | 185 | 892 |
| 383.xx | 377 | 868 |
| 125.xx | 123 | 864 |
| 136.xx | 137 | 854 |
| 412.xx | 410 | 849 |
| 263.xx | 256 | 819 |
| 394.xx | 397 | 810 |
| 346.xx | 328 | 701 |
| 396.xx | 398 | 688 |
| 300.xx | 308 | 605 |
| 200.xx | 208 | 566 |
| 343.xx | 318 | 553 |
| 202.xx | 206 | 543 |
| 335.xx | 317 | 518 |
| 265.xx | 276 | 491 |
| 482.xx | 459 | 484 |
| 247.xx | 244 | 476 |
| 100.xx | 108 | 453 |
| 142.xx | 149 | 447 |
| 263.xx | 269 | 425 |
| 256.xx | 276 | 424 |
| 253.xx | 258 | 391 |
| 267.xx | 285 | 387 |
| 392.xx | 388 | 332 |
| 347.xx | 329 | 327 |
| 310.xx | 308 | 303 |
| 182.xx | 188 | 287 |
| 268.xx | 286 | 246 |
| 491.xx | 490 | 210 |
| 393.xx | 392 | 189 |
| 141.xx | 148 | 183 |
| 145.xx | 158 | 145 |
| 302.xx | 300 | 133 |
| 127.xx | 145 | 81 |
| 343.xx | 328 | 79 |
| 231.xx | 229 | 61 |
| 422.xx | 423 | 54 |
| 382.xx | 368 | 46 |
| 176.xx | 177 | 30 |
| 452.xx | 440 | 29 |
| 456.xx | 442 | 23 |
| 262.xx | 261 | 15 |
| 387.xx | 378 | 15 |
| 191.xx | 196 | 15 |
| 256.xx | 269 | 11 |
| 175.xx | 180 | 11 |
| 465.xx | 443 | 9 |
| 453.xx | 441 | 9 |
| 382.xx | 369 | 9 |
| 145.xx | 159 | 8 |
| 481.xx | 459 | 6 |
| 262.xx | 278 | 5 |
| 145.xx | 150 | 5 |
| 103.xx | 102 | 3 |
| 305.xx | 320 | 3 |
| 357.xx | 351 | 3 |
| 127.xx | 126 | 2 |
| 183.xx | 186 | 2 |
| 116.xx | 112 | 2 |
| 126.xx | 127 | 2 |
| 122.xx | 123 | 1 |
| 264.xx | 262 | 1 |
| 130.xx | 131 | 1 |
| 131.xx | 134 | 1 |
| 255.xx | 259 | 1 |
| 313.xx | 312 | 1 |
| 252.xx | 248 | 1 |
| 401.xx | 402 | 1 |
| 394.xx | 396 | 1 |
| 396.xx | 397 | 1 |
| 376.xx | 374 | 1 |
| 482.xx | 483 | 1 |
| 145.xx | 149 | 1 |
| 107.xx | 106 | 1 |
| 480.xx | 123 | 1 |
| 307.xx | 414 | 1 |
| 336.xx | 337 | 1 |
| 311.xx | 308 | 1 |
| 151.xx | 150 | 1 |
| 473.xx | 457 | 1 |
| 483.xx | 481 | 1 |
| 13.xx | 139 | 1 |
| **92 pairs** | | **32,067** |

## F. UnregisteredAddresses points on retired 2021 precinct numbers

| Retired precinct | Address points |
|---|---:|
| 124 | 1,307 |
| 393 | 415 |
| 461 | 384 |
| 194 | 360 |
| 125 | 298 |
| 202 | 266 |
| 412 | 243 |
| 263 | 225 |
| 373 | 214 |
| 133 | 210 |
| 136 | 204 |
| 431 | 201 |
| 224 | 199 |
| 183 | 178 |
| 302 | 105 |
| 151 | 104 |
| 247 | 101 |
| 310 | 87 |
| 265 | 58 |
| 422 | 18 |
| 176 | 17 |
| 175 | 9 |
| **22 numbers** | **5,203** |

## G. Precinct 369 — nine voters published under an invalid precinct

Precinct 369 is a 1.68-acre precinct created by splitting 2021 precinct 382. Its four appraisal parcels are:

| Property ID | Address | ZIP |
|---|---|---|
| 156309 | 5302 Hanging Cliff Cv | 78759 |
| 156310 | 5308 Hanging Cliff Cv | 78759 |
| 156311 | 10701 Watering Rock Ln | 78759 |
| 156313 | 10705 Watering Rock Ln | 78759 |

The address master places nine address points in precinct 369 (those four plus 5300 and 5306 Hanging Cliff Cove, 10703 Watering Rock Lane, and 10725 and 10727 Floral Park Drive), all nine coded `PrecinctSplit` 382.01.

These nine registrations were in precinct 369 on every county Voter List from February 10, 2026 to October 13, 2026, on the September 16, 2026 OLRV, and on the October 27, 2026 **initial** OLRV. Then:

- the **October 20, 2026 Voter List** gives their precinct as **"P 182.4"** — a precinct-split code, not a precinct number. Precinct 182 exists in both the 2021 and 2025 plans and lies about 7 miles east, in congressional district G10 rather than G37, house district L50 rather than L47 and commissioners precinct P01 rather than P03. These are the only nine values of the form `nnn.n` in a `Precinct` column of 950,019 rows;
- the **October 27, 2026 OLRV** (`2026-10-27_G26_journal_gen_voter.csv`) gives their precinct as **"P 309"**. There is no precinct 309 in the 308-precinct plan.

Precinct 369 consequently shows **0 registered voters** on both files while **6 of these voters appear on the G26 roster in precinct 369** — the only precinct in the county where roster voters exceed registrations.

**VUIDs to correct to precinct 369:**

| VUID | Oct 20 Voter List | Oct 27 OLRV | Voted G26 |
|---|---|---|---|
| 1139710425 | P 182.4 | P 309 | yes (roster 369) |
| 1139750188 | P 182.4 | P 309 | no |
| 1139750190 | P 182.4 | P 309 | no |
| 1140608191 | P 182.4 | P 309 | yes (roster 369) |
| 1140608232 | P 182.4 | P 309 | yes (roster 369) |
| 1140685779 | P 182.4 | P 309 | yes (roster 369) |
| 1140703276 | P 182.4 | P 309 | yes (roster 369) |
| 1140708900 | P 182.4 | P 309 | no |
| 2168122786 | P 182.4 | P 309 | yes (roster 369) |

**The correct value did reach the November election.** All six of these voters who voted appear on the G26 roster under precinct 369, so the roster the polls worked from carried the right precinct even though both published lists did not. This is a defect in the published extracts, not in the ballots issued.

**Requested actions.** Confirm that all nine registrations now read precinct 369 on any extract taken today; reissue the October 20 Voter List and the October 27 OLRV extract with a valid precinct value; and determine whether the "182.4" and "309" values were introduced by the manual correction of the roughly 200 incorrect-split voters the county described, so the same defect can be prevented at the next extract.

## H. Precinct "99999"

The **October 13, 2026** Voter List places 14 registrations in precinct **"P 99999"**. The value matches no precinct in the 2021 or 2025 plan. Eight are in suspense status. None carries that value on the October 20 list, and all but two have a real precinct on the October 27 OLRV.

| VUID | Status Oct 13 | Precinct on the Oct 27 OLRV | Address problem |
|---|---|---|---|
| 1143223655 | Active | S 154.7 | residence address reads "8915 MISSING ADDRESS" |
| 2134244409 | Suspense | not on the OLRV | residence address is in Fort Worth |
| 2215448552 | Active | not on the OLRV | — |
| 1173169137 | Active | S 346.2 | — |
| 2123122373 | Active | S 153.7 | — |
| 2143311700 | Active | S 357.4 | — |
| 2160408429 | Active | P 351 | — |
| 2205414071 | Active | S 204.3 | — |
| 2125479362 | Suspense | S 142.2 | — |
| 2135800755 | Suspense | S 410.3 | — |
| 2202924771 | Suspense | S 459.2 | — |
| 2206654437 | Suspense | P 435 | — |
| 2222389089 | Suspense | P 312 | — |
| 3382352206 | Suspense | P 400 | — |

**Requested actions.** Confirm what "99999" denotes in the extract, resolve the two registrations that reach no precinct on the OLRV, and correct the two unusable residence addresses. The county previously told us that "P99999 voters were not pulled in" to the October 27 initial OLRV and that the current OLRV "first included P99999 voters, which should not be on the OLRV and were deleted"; these 14 records are the population that statement refers to.

## I. Precinct size and empty precincts

### I.1 Below the §42.006(a) minimum of 100 registered voters

Counts are non-suspense registrations from the October 20, 2026 Voter List, as §42.006(e) directs ("voters whose names appear on the list of registered voters with the notation 'S' … shall be excluded").

| Precinct | Non-suspense | Registered | Address points | Parcels | Acres |
|---|---:|---:|---:|---:|---:|
| 128 | 0 | 0 | 1 placeholder | 0 | 1.74 |
| 139 | 0 | 0 | 1 placeholder | 0 | 5.87 |
| 147 | 0 | 0 | 1 placeholder | 0 | 3.20 |
| 179 | 0 | 0 | 1 placeholder | 0 | 0.60 |
| 228 | 0 | 0 | 1 placeholder | 0 | 3.47 |
| 239 | 0 | 0 | 1 placeholder | 0 | 25.81 |
| 249 | 0 | 0 | 1 placeholder | 0 | 11.88 |
| 257 | 0 | 0 | 1 placeholder | 0 | 3.47 |
| 259 | 0 | 0 | 1 placeholder | 0 | 1.83 |
| 369 | 0 | 0 | 9 | 4 | 1.68 |
| 159 | 1 | 1 | 9 | 11 | 613.56 |
| 248 | 2 | 2 | 2 | 2 | 22.31 |
| 443 | 3 | 3 | 9 | 9 | 70.19 |
| 441 | 10 | 11 | 9 | 8 | 3.53 |
| 440 | 37 | 40 | 29 | 0 | 3.80 |
| 368 | 70 | 94 | 46 | 52 | 48.54 |
| 229 | 95 | 104 | 61 | 61 | 421.87 |

Under the 2021 plan only one of 287 precincts (175, with 2 voters) was below 100.

### I.2 Above the §42.006(a) maximum of 5,000 registered voters

| Precinct | Registered | Non-suspense | Over by | `VoterCount` at adoption |
|---|---:|---:|---:|---:|
| 126 | 6,271 | 5,666 | 666 | 5,387 |
| 464 | 6,319 | 5,580 | 580 | 5,261 |
| 138 | 6,068 | 5,439 | 439 | 5,161 |
| 483 | 5,746 | 5,318 | 318 | 5,102 |
| 337 | 6,212 | 5,115 | 115 | 4,756 |
| 141 | 5,516 | 5,067 | 67 | 4,707 |

Four of the six already exceeded 5,000 on the county's own `VoterCount` attribute when the plan was adopted.

### I.3 Precincts that contain no residence

Each of these nine precincts contains exactly one placeholder address record (of the form "*nn* FAKE PT") and no appraisal parcel at all.

| Precinct | Acres | Dimensions | What is inside | Cong. | Comm. | JP |
|---|---:|---|---|---|---|---|
| 128 | 1.74 | 674 × 229 ft | FM 969 | G27 | P01 | J01 |
| 139 | 5.87 | 1,372 × 232 ft | Harris Branch Pkwy / E Howard Ln | G10 | P01 | J01 |
| 147 | 3.20 | 857 × 170 ft | no road | G10 | P01 | J01 |
| 179 | 0.60 | 412 × 74 ft | Dessau Rd | G10 | P01 | J02 |
| **228** | 3.47 | **2,574 × 125 ft** | **Loop 1 / MoPac Expressway main lanes**, between Enfield Rd and Windsor Rd | G37 | P02 | J05 |
| **239** | 25.81 | **8,155 × 965 ft** | **Lake Austin; 100% water. 93 ft of Loop 360 at the Pennybacker Bridge** | G10 | P02 | J05 |
| 249 | 11.88 | 2,245 × 336 ft | no road | G37 | P02 | J01 |
| 257 | 3.47 | 1,623 × 98 ft | E Parmer Ln / E Yager Ln | G37 | P02 | J01 |
| 259 | 1.83 | 822 × 243 ft | Wells Branch Pkwy / W Howard Ln | G10 | P02 | J02 |

**Requested actions.** Under §42.010(b) the Commissioners Court is directed to consider eliminating precincts with no population after a redistricting of a commissioners or justice precinct, and under §42.010(c) it may submit recommendations to the Secretary of State after congressional, house, senate or State Board of Education redistricting. The nine above and precincts 159, 248 and 443 meet the §42.010(d) definition of a "substantially small population" (not more than 6 registered voters). For the six oversize precincts, §42.031(a) sets the next review for March–April 2027, and §42.031(b)(2) permits an earlier order specifically to bring a precinct under the maximum. §42.0051(a) permits combining precincts under 3,000 registered voters that result from redistricting.

Any precinct the county keeps should also be flagged in the registration system as one that cannot have a residential registration, so that an assignment into precinct 128, 139, 147, 179, 228, 239, 249, 257 or 259 is rejected at entry.

## J. TEAM OLRV precinct disagreements

The October 27, 2026 OLRV has **489 voters at 476 addresses** whose precinct disagrees with the county address master. They are listed by VUID in records section S2.

Two groups need different handling:

- **The same clusters that are wrong on the county Voter List**, which should be corrected outright: split `131.3` where the address master says 357 (17 voters), `121.2` against 357 (7), `370.3` against 292 (8), `309` against 369 (9), `300` against 411 (4), `411` against 301 (4).
- **Dense student-housing addresses** around the University of Texas, in precincts 204, 206, 308, 311, 312, 313 and 314. One street address there can genuinely span precincts, and the OLRV gives only a single free-text address line with no separate unit field, so these need the county's unit-level data to resolve. They are reported for confirmation, not asserted as errors.

One OLRV precinct value is corrupt outright: **VUID 3383028111** appears in split **"0864.02"**. The September 16 OLRV and the October 27 initial OLRV both place the same voter in split **308.3**, and the October 13 and October 20 county Voter Lists both give precinct **308**.

## Suggested systemic fixes

1. **Directional-prefix validation.** The five largest misassignment clusters in section A all come from a registration address recorded without its N/S/E/W prefix, resolved onto the wrong side of the county: 8000, 7880 and 7631 US 290 in ZIP 78736 (coded to precincts 131 and 121, which serve **E** US 290 in 78754, instead of 357 for **W** US 290); 3501 FM 620 in 78738 (coded to 370 for **N** FM 620 in 78734 instead of 292 for **S** FM 620); and 1007 Congress in 78704 (coded to 300 for downtown Congress Avenue in 78701 instead of 411 for **S** Congress Avenue). The address master already carries the county's own warnings on the wrong-side records — "Rogue" on 7631, 7880 and 8000 E US 290 HWY, and "Rogue. Bad address" on 3501 N FM 620 RD. Two rules would close this: reject a geocode onto any address-master point the county has annotated "Rogue" or "Bad Address", and require the directional prefix whenever the ZIP code is inconsistent with the candidate point.
2. **Cross-check the registration precinct against the address master at entry and on every extract.** Running the comparison in this supplement against the full list takes a few minutes and would have caught all 141 county-list errors and the precinct 369 failure before the October 20 publication.
3. **Validate the `Precinct` column's format.** No precinct value should contain a decimal point, and every value should be one of the 308 adopted numbers. That single check would have rejected "P 182.4", "P 309", "S 0864.02" and "P 99999".
4. **Complete the 2025 migration in the GIS layers.** Sections E and F list the stale numbering. The `Splits` polygon layer in particular still describes the 2021 plan's 287 precincts in all 1,133 polygons and needs redrawing; because the OLRV's precinct field is itself a split designation, this is the most plausible source of the late-October precinct corruptions.
5. **Complete the published shapefile's metadata.** `Election_Precincts.shp.xml` still carries the unmodified template text "REQUIRED: A brief narrative summary of the data set" for both abstract and purpose. Adding the December 4, 2025 order date, the January 1, 2026 effective date, a plan name, and a note that `VoterCount` is a point-in-time snapshot (it is now below the current registered count in 294 of 308 precincts) would prevent misreadings of the published file.
6. **Populate `Latitude` and `Longitude` in `AdMas`.** They are 0 in 410,070 of 412,053 records, so the layer's own coordinate columns cannot be used and the point geometry must be downloaded instead.
7. **Fix two housekeeping items in `AdMas`.** 532 records have a blank `County` code, and the test record "51 FAKE PT" has county code "277" rather than Travis's 227.
8. **Link the address-level layers from the Voter Registration Maps hub's download page.** `AdMas`, `VotingAddresses` and `UnregisteredAddresses` are what make precinct assignment auditable by the public, and they are already served publicly, but the hub's downloads page does not present them alongside the precinct shapefile.

