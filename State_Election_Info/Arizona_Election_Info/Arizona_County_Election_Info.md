# Arizona County Election Information

For each of the 15 Arizona counties: the county elections director and the County Recorder (who maintains voter registration), per the Arizona SOS county election contact list; whether a registered-voter list can be downloaded; the elections website; the historical election-results location and the record types posted for 2020-present; the public-records route; and the voting equipment in use, per the SOS 2026 Election Cycle Voting Equipment list.

Researched 2026-10-09. Machine-readable copies: `arizona_county_election_info.json` (canonical) and `arizona_county_election_info.csv`.

## Summary

- **Counties:** 15
- **Voting system:** Election Systems & Software (ES&S): 13 counties; Liberty Vote (formerly Dominion Voting Systems): 1 counties; Unisyn Voting Solutions: 1 counties. Per the SOS 2026 Election Cycle Voting Equipment list.
- **Voter list:** No free public download in any county. County Recorders furnish voter lists for political or election purposes, for a fee (A.R.S. 16-168).
- **County results posted on county sites:** 15 counties.
- **Cast vote records posted on county sites:** none found.
- **Ballot images posted on county sites:** none found.
- **EMS audit logs posted online:** Cochise, Coconino, Maricopa, Mohave, Navajo, Pima, Pinal, Santa Cruz, Yavapai.
- **Ballot transfer / chain-of-custody logs posted online:** none found.
- **Results tapes / zero reports posted online:** 0 counties.
- **Voter rosters / numbered lists posted online:** none found.
- **Post-election audit reports posted online:** 4 counties.
- **Reconciliation reports posted online:** 0 counties.
- **Open records:** 7 counties have an online request portal or request page; the rest take written public records requests (A.R.S. 39-121) by mail, email or in person.

## Statewide resources

- County election contacts: https://azsos.gov/elections/about-elections/county-election-contact-info
- Voting equipment by county 2026: https://azsos.gov/sites/default/files/docs/2026-Election-Cycle-Voting-Equipment--Primary-Election.pdf
- Voting equipment list revised: July 2026
- Voting equipment certification: https://azsos.gov/elections/about-elections/elections-procedures/voting-equipment
- Elections procedures manual: https://azsos.gov/elections/about-elections/elections-procedures/epm
- Post election procedures: https://azsos.gov/elections/about-elections/elections-procedures/post-election-procedures
- Election info 2026: https://azsos.gov/elections/election-information/2026-election-info
- Election info 2024: https://azsos.gov/elections/election-information/2024-election-info
- Hand count audit summary 2022 primary: https://azsos.gov/elections/results-data/election-information/2022-election-information/summary-hand-count-audits
- Statewide results: https://results.arizona.vote/
- Voter registration statistics: https://azsos.gov/elections/election-information/voter-registration-statistics

## Notes

- Contacts come from the Arizona SOS county election contact page (https://azsos.gov/elections/about-elections/county-election-contact-info), retrieved 2026-10-09. Each county has two offices: the County Recorder, who handles voter registration and early voting (voter_registrar), and the elections department under the Board of Supervisors, which runs elections and tabulation (election_official). In Pinal County the Recorder now runs both. Maricopa lists two election directors; the second is named in historical_results.notes. The SOS page hides emails behind Cloudflare email protection; they were decoded, and names left in HTML comments (former officials) were ignored. Apache County's elections email is commented out on the SOS page, so it is null.
- Voting equipment comes from the SOS '2026 Election Cycle / Voting Equipment' list (Primary Election, revised July 2026), which the SOS says may be updated before the next election. Vendors: Election Systems & Software (ES&S): 13; Liberty Vote (formerly Dominion Voting Systems): 1; Unisyn Voting Solutions: 1. Maricopa uses Liberty Vote (formerly Dominion) Democracy Suite 5.20 with ImageCast Precinct 2 vote-center tabulators and ImageCast Central scanners; Yavapai uses Unisyn OpenElect 2.2; the other 13 counties use ES&S ElectionWare (6.5, or 6.3 in Santa Cruz). Arizona voters mark paper ballots by hand, with ballot-marking devices for accessibility. tabulation.equipment lists each row of the SOS table with its firmware and software versions; quantities are not published.
- Post-election hand count audits: after each primary and general election, A.R.S. 16-602 requires a hand count of a sample of precincts/vote centers and early ballots, by boards whose members are designated by the political parties. If the parties do not designate board members, the audit is not performed. The SOS posts each county's hand count report on its election-information pages; for the 2026 primary, reports for all 15 counties are linked from https://azsos.gov/elections/election-information/2026-election-info. Counties that also post the report on their own site are recorded under hand_count_audit.
- Several counties post the EMS audit-event or system log for each election alongside their results: Cochise, Coconino, Maricopa, Mohave, Navajo, Pima, Pinal, Santa Cruz, Yavapai. No county was found posting cast vote records or ballot images.
- Voter lists: there is no public voter-file download. Under A.R.S. 16-168 the County Recorder (or the SOS) furnishes voter lists, for a fee and on a signed request, to candidates, political parties, committees and others for political or election purposes; the lists may not be used for commercial purposes.
- Hand counting in place of machines: in 2022 Cochise County's Board of Supervisors voted to hand count all ballots; a court blocked the full hand count, and two supervisors were later charged over the delayed canvass. Tabulation in every county is by optical scan.
- records_found_2020_present lists only records found posted on the county's own sites. It is not exhaustive, and absence means 'not found'. Years come from link text, file names and upload folders.
- Public records: requests go to the county under A.R.S. 39-121 et seq. open_records.url is the county's online portal or request page where one was found.
- Deep links change with site redesigns. Start from county_website or the SOS county election contact page if a link breaks.

## Record-type codes

Years are two-digit (`22-24` = 2022–2024); `undated` = items whose year could not be inferred.

| Code | Record type |
|------|-------------|
| RES | Election results (summary / precinct / statement of votes cast) |
| CAN | Official canvass |
| REC | Reconciliation reports |
| HCA | Post-election hand count audit (A.R.S. 16-602) |
| ROS | Voter rosters / lists of voters |
| TAPE | Results tapes / zero & totals reports |
| HASH | Hash validation / L&A test reports |
| LOG | EMS audit / event or system logs |
| XFER | Ballot transfer / chain-of-custody logs |
| CVR | Cast vote records (CVR) |
| IMG | Ballot images / scanned ballots |
| VF | Full registered-voter file |

## Counties

| # | County | Election official | Voter registrar | Voter list | Elections site | Historical results | Records posted (2020–present) | Open records | Tabulation |
|---|--------|-------------------|-----------------|------------|----------------|--------------------|-------------------------------|--------------|------------|
| 1 | [Apache](https://www.apachecountyaz.gov/) | Apache County Elections Director<br>Megan Hill<br>928-337-7604 | Apache County Recorder<br>Larry Noble<br>[site](https://www.apachecountyaz.gov/Recorder/) | Request ([form/info](https://www.apachecountyaz.gov/Recorder/)) | [elections](https://www.apachecountyaz.gov/Elections/) | [results](https://www.apachecountyaz.gov/historical-elections-results) | RES 20-24<br>CAN 22, 24 | Written public records request | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 2 | [Cochise](https://www.cochise.az.gov/) | Cochise County, Elections Director<br>Melissa Avant<br>520-432-8975 or 1-888-316-8065 | Cochise County Recorder<br>Billy Cloud<br>[site](https://www.cochise.az.gov/recorder/home) | Request ([form/info](https://www.cochise.az.gov/recorder/home)) | [elections](https://www.cochise.az.gov/292/Elections) | [results](https://www.cochise.az.gov/353/Election-Results) | RES 26; undated<br>LOG 26; undated | [portal/page](https://cochise-az.nextrequest.com/) | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 3 | [Coconino](https://www.coconino.az.gov/) | Coconino County Elections Director<br>Eslir Musta<br>928-679-7896 or 1-800-793-6181 | Coconino County Recorder<br>Aubrey Sonderegger<br>[site](https://www.coconino.az.gov/319/Recorder) | Request ([form/info](https://www.coconino.az.gov/319/Recorder)) | [elections](https://www.coconino.az.gov/195/Elections) | [results](https://www.coconino.az.gov/205/Past-Election-Results)<br>Enhanced Voting | RES 22-26<br>LOG 26 | Written public records request | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 4 | [Gila](https://www.gilacountyaz.gov/) | Gila County Elections Director<br>Eric A. Mariscal<br>928-402-8709 | Gila County Recorder<br>Wendy Mannigal-Smith<br>[site](https://www.gilacountyaz.gov/government/recorder/) | Request ([form/info](https://www.gilacountyaz.gov/government/recorder/)) | [elections](https://www.gilacountyaz.gov/government/elections/) | [results](https://www.gilacountyaz.gov/government/elections/election_results_-_historical.php) | RES 20, 22-26<br>CAN 20, 22-26 | Written public records request | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 5 | [Graham](https://www.graham.az.gov/) | Graham County Elections Director<br>Hannah Duderstadt<br>928-792-5037 | Graham County Recorder<br>Polly Merriman<br>[site](https://www.graham.az.gov/239/Recorder) | Request ([form/info](https://www.graham.az.gov/239/Recorder)) | [elections](https://www.graham.az.gov/476/Elections) | [results](https://www.graham.az.gov/509/Election-Results) | RES 24, 26 | Written public records request | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 6 | [Greenlee](https://greenlee.az.gov/) | Greenlee County Elections Director<br>Bianca Castañeda<br>928-865-2072 | Greenlee County Recorder<br>Erin Miller<br>[site](https://greenlee.az.gov/elected-officials/recorder/) | Request ([form/info](https://greenlee.az.gov/elected-officials/recorder/)) | [elections](https://greenlee.az.gov/ova_dep/elections/) | [results](https://greenlee.az.gov/ova_dep/elections/)<br>Clarity ENR | RES 26<br>CAN 26 | [portal/page](https://greenlee.az.gov/public-records-request-contacts/) | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 7 | [La Paz](https://www.lapaz.gov/) | La Paz County Elections Director<br>Tracy Page<br>928-669-6149 | La Paz County Recorder<br>Richard Garcia<br>[site](https://www.lapaz.gov/213/Recorder) | Request ([form/info](https://www.lapaz.gov/213/Recorder)) | [elections](https://www.lapaz.gov/162/Elections) | [results](https://www.lapaz.gov/557/Election-Results) | RES 20, 22, 24-25; undated<br>CAN 20, 22, 24; undated<br>HCA undated | Written public records request | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 8 | [Maricopa](https://www.maricopa.gov/) | Maricopa County Director of Election Services and Early Voting<br>Rey Valenzuela<br>602-506-1511 | Maricopa County Recorder<br>Justin Heap<br>[site](https://recorder.maricopa.gov/) | Request ([form/info](https://recorder.maricopa.gov/)) | [elections](https://elections.maricopa.gov/) | [results](https://elections.maricopa.gov/results-and-data/historic-results.html) | RES 20-26<br>CAN 20-26<br>LOG 24-26 | [portal/page](https://elections.maricopa.gov/news-and-information/public-records-request.html) | Liberty Vote (formerly Dominion Voting Systems) Democracy Suite V5.20.2.6 |
| 9 | [Mohave](https://www.mohave.gov/) | Mohave County Elections Director<br>Allen P. Tempert<br>928-753-0733 | Mohave County Recorder<br>Lydia Henry<br>[site](https://www.mohave.gov/departments/recorder/) | Request ([form/info](https://www.mohave.gov/departments/recorder/)) | [elections](https://www.mohave.gov/departments/elections) | [results](https://www.mohave.gov/departments/elections/election-results/) | RES 20-22, 24, 26; undated<br>CAN 20-22, 24; undated<br>LOG undated | [portal/page](https://mohavecountyaz.mycusthelp.com/WEBAPP/_rs/supporthome.aspx) | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 10 | [Navajo](https://www.navajocountyaz.gov/) | Navajo County Elections Director<br>Rayleen Richards<br>928-524-4062 | Navajo County Recorder<br>David Marshall<br>[site](https://www.navajocountyaz.gov/299/Recorder) | Request ([form/info](https://www.navajocountyaz.gov/299/Recorder)) | [elections](https://www.navajocountyaz.gov/223/Elections) | [results](https://www.navajocountyaz.gov/506/Election-Results) | RES 20-26<br>CAN 24; undated<br>LOG undated | Written public records request | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 11 | [Pima](https://www.pima.gov/) | Pima County Elections Director<br>Constance Hargrove<br>520-724-6830 | Pima County Recorder<br>Gabriella Cázares-Kelly<br>[site](https://www.recorder.pima.gov/RecorderHome) | Request ([form/info](https://www.recorder.pima.gov/RequestVoterData.aspx)) | [elections](https://www.pima.gov/elections) | [results](https://www.pima.gov/election-results) | RES 20-26<br>CAN 20, 24-26; undated<br>HCA 20, 25-26<br>LOG 25 | Written public records request | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 12 | [Pinal](https://www.pinal.gov/) | County Recorder (runs Pinal County Elections)<br>Dana Lewis<br>520-866-7550 | Pinal County Recorder<br>Dana Lewis<br>[site](https://www.pinal.gov/votes) | Request ([form/info](https://www.pinal.gov/votes)) | [elections](https://www.pinal.gov/258/Elections) | [results](https://www.pinal.gov/280/Election-Results) | RES 20-26<br>CAN 25-26; undated<br>LOG 25-26 | [portal/page](https://pinalcounty-az.nextrequest.com/) | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |
| 13 | [Santa Cruz](https://www.santacruzcountyaz.gov/) | Santa Cruz County Elections Director<br>Alma Schultz<br>520-375-7808 | Santa Cruz County Recorder<br>Anita Moreno<br>[site](https://www.santacruzcountyaz.gov/287/Recorder) | Request ([form/info](https://www.santacruzcountyaz.gov/287/Recorder)) | [elections](https://www.santacruzcountyaz.gov/173/Elections) | [results](https://www.santacruzcountyaz.gov/175/Election-Results) | RES 20, 22-26<br>CAN 24, 26<br>LOG 26 | [portal/page](https://www.santacruzcountyaz.gov/842/Public-Records-Request) | Election Systems & Software (ES&S) ElectionWare 6.3.0.0 |
| 14 | [Yavapai](https://www.yavapaiaz.gov/) | Yavapai County Director of Elections<br>Laurin Custis<br>928-771-3248 Option 8 | Yavapai County Recorder<br>Michelle Burchill<br>[site](https://yavapaivotes.gov) | Request ([form/info](https://yavapaivotes.gov)) | [elections](https://www.yavapaivotes.gov/) | [results](https://www.yavapaivotes.gov/Elections/Election-Results-Livestream) | RES 20-26<br>CAN 20-26<br>HCA 26<br>LOG 24-26 | Written public records request | Unisyn Voting Solutions OCS OpenElect 2.2 |
| 15 | [Yuma](https://www.yumacountyaz.gov/) | Director<br>Desiree Phillips<br>928-373-1014 | Yuma County Recorder<br>David Lara<br>[site](https://www.yumacountyaz.gov/government/recorder) | Request ([form/info](https://www.yumacountyaz.gov/government/recorder)) | [elections](https://www.yumacountyaz.gov/government/voter-election-services) | [results](https://www.yumacountyaz.gov/government/voter-election-services/election-results) | RES 20-26<br>HCA undated | [portal/page](https://www.yumacountyaz.gov/government/election-services/election-services-public-record-request) | Election Systems & Software (ES&S) ElectionWare 6.5.0.0 |

## County notes

- **Cochise:** In 2022 the Cochise County Board of Supervisors voted to hand count all ballots; a court blocked the full hand count, and two supervisors were later charged over the delayed 2022 canvass.
- **Maricopa:** The SOS also lists Scott Jarett, Maricopa County Director of Election Day and Emergency Voting, 602-506-1511, voterinfo@maricopa.gov.
- **Pinal:** In Pinal County the County Recorder runs elections as well as voter registration.
