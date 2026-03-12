"""
sic_discovery.py — Automatic SIC code discovery from a free-text sector description.

Given a rough sector/niche description (e.g. "fire safety", "electrical contractors",
"waste management"), this module:
  1. Checks a curated sector→SIC keyword map (highest accuracy)
  2. Falls back to keyword + fuzzy scoring against the full SIC list
  3. Auto-generates a complete config object ready to plug into the pipeline

No manual config editing required.
"""

import os
import re
import json
import time
import requests
from difflib import SequenceMatcher

# ── Curated sector keyword → SIC code map ─────────────────────────────────────
# Primary matching layer — much more accurate than fuzzy matching alone.
# Each entry is: keyword_triggers → {sic_codes, benchmark_category, market_score}
# Triggers are matched case-insensitively against the user's sector description.
CURATED_SECTORS = [
    {
        "triggers": ["fire safety", "fire protection", "fire alarm", "fire suppression",
                     "fire sprinkler", "fire extinguish", "fire system", "passive fire",
                     "fire detection", "fire prevention", "active fire"],
        "sic_codes": ["80200", "43210", "43290", "33190", "71200", "74909"],
        "benchmark_category": "technical_services",
        "market_score": 82,
    },
    {
        "triggers": ["security system", "cctv", "access control", "intruder alarm",
                     "security installation", "electronic security", "surveillance",
                     "alarm system", "security guard", "security service"],
        "sic_codes": ["80200", "80100", "80300", "43210", "43290"],
        "benchmark_category": "technical_services",
        "market_score": 78,
    },
    {
        "triggers": ["electrical contractor", "electrical installation", "electrical engineer",
                     "electrician", "electrical testing", "pat testing", "wiring",
                     "electrical maintenance", "electrical service", "power installation",
                     "electrical inspection", "condition report"],
        "sic_codes": ["43210", "71200", "33140", "27120"],
        "benchmark_category": "construction_trades",
        "market_score": 75,
    },
    {
        "triggers": ["ev charging", "electric vehicle", "ev charger", "ev install",
                     "charge point", "charging infrastructure"],
        "sic_codes": ["43210", "35130", "27120", "33140"],
        "benchmark_category": "technical_services",
        "market_score": 85,
    },
    {
        "triggers": ["plumbing", "hvac", "heating", "ventilation", "air conditioning",
                     "boiler", "heat pump", "mechanical services", "m&e", "mechanical electrical",
                     "gas engineer", "gas installation", "central heating", "pipework"],
        "sic_codes": ["43220", "43290", "33190", "28250", "35300"],
        "benchmark_category": "construction_trades",
        "market_score": 74,
    },
    {
        "triggers": ["calibration", "metrology", "calibration lab", "dimensional measurement",
                     "calibration service", "instrument calibration", "torque calibration",
                     "pressure calibration", "ndt", "non-destructive testing", "non destructive"],
        "sic_codes": ["71200", "33190", "33130", "26511", "26513", "74909", "71122"],
        "benchmark_category": "technical_services",
        "market_score": 78,
    },
    {
        "triggers": ["inspection service", "technical inspection", "testing laboratory",
                     "quality testing", "conformity assessment", "third party inspection",
                     "ndt inspection", "inspection and testing"],
        "sic_codes": ["71200", "74909", "71122", "33190"],
        "benchmark_category": "technical_services",
        "market_score": 80,
    },
    {
        "triggers": ["waste management", "waste disposal", "waste collection", "recycling",
                     "hazardous waste", "skip hire", "waste contractor", "environmental waste",
                     "clinical waste", "waste treatment"],
        "sic_codes": ["38110", "38120", "38210", "38220", "38310", "38320", "39000"],
        "benchmark_category": "waste_environmental",
        "market_score": 80,
    },
    {
        "triggers": ["environmental consulting", "environmental service", "environmental remediation",
                     "soil remediation", "contaminated land", "ecology survey", "environmental assessment",
                     "air quality", "noise survey", "environmental monitoring"],
        "sic_codes": ["74901", "71200", "39000", "74909", "71122"],
        "benchmark_category": "technical_services",
        "market_score": 82,
    },
    {
        "triggers": ["it support", "managed service", "msp", "it managed", "helpdesk",
                     "it service", "it maintenance", "network support", "infrastructure",
                     "server support", "cloud service", "it outsourc"],
        "sic_codes": ["62020", "62030", "62090", "63110"],
        "benchmark_category": "it_digital",
        "market_score": 80,
    },
    {
        "triggers": ["software development", "bespoke software", "software house",
                     "web development", "app development", "digital agency", "saas",
                     "software engineer", "technology solutions"],
        "sic_codes": ["62012", "62011", "62020", "62090", "63110"],
        "benchmark_category": "it_digital",
        "market_score": 82,
    },
    {
        "triggers": ["cybersecurity", "cyber security", "penetration testing", "pen test",
                     "information security", "soc", "security operations", "vulnerability",
                     "data protection", "gdpr consulting"],
        "sic_codes": ["62020", "80200", "62090", "63110"],
        "benchmark_category": "it_digital",
        "market_score": 88,
    },
    {
        "triggers": ["roofing", "flat roof", "cladding", "waterproofing", "roof contractor",
                     "roof repair", "roof installation"],
        "sic_codes": ["43910", "43999", "43290"],
        "benchmark_category": "construction_trades",
        "market_score": 68,
    },
    {
        "triggers": ["scaffolding", "scaffold", "temporary works", "access equipment"],
        "sic_codes": ["43991", "43999"],
        "benchmark_category": "construction_trades",
        "market_score": 70,
    },
    {
        "triggers": ["civil engineering", "groundwork", "groundworks", "earthworks",
                     "infrastructure contractor", "utilities contractor"],
        "sic_codes": ["42110", "42210", "42220", "42990", "43120"],
        "benchmark_category": "construction_trades",
        "market_score": 72,
    },
    {
        "triggers": ["drainage", "sewer", "sewerage", "drain cleaning", "drain survey",
                     "CCTV drain", "drain inspection", "utility mapping"],
        "sic_codes": ["37000", "42210", "43120", "43999"],
        "benchmark_category": "construction_trades",
        "market_score": 74,
    },
    {
        # Passenger/goods lifts, platform lifts, escalators — building-installed vertical transport
        # SEPARATE from industrial lifting/crane/forklift (below)
        "triggers": ["lift maintenance", "lift service", "lift installation",
                     "lift engineer", "lift contractor", "lift company",
                     "passenger lift", "platform lift", "stairlift", "dumbwaiter",
                     "elevator", "escalator", "leia member"],
        "sic_codes": ["43290", "33120", "28221", "81100"],
        "benchmark_category": "technical_services",
        "market_score": 76,
        "exclude_subsectors": [
            "marine", "boat", "vessel", "ship", "yacht", "pontoon", "barge",
            "dock", "watercraft", "crane", "forklift", "pallet", "hoist",
            "conveyor", "mining",
        ],
    },
    {
        "triggers": ["lifting equipment", "crane hire", "hoist", "rigging",
                     "material handling", "forklift", "plant hire"],
        "sic_codes": ["28220", "77320", "43999", "33120"],
        "benchmark_category": "technical_services",
        "market_score": 72,
        "exclude_subsectors": ["marine", "boat", "vessel", "ship", "yacht"],
    },
    {
        "triggers": ["engineering consultancy", "engineering design", "process engineering",
                     "mechanical engineering consultancy", "structural engineering"],
        "sic_codes": ["71121", "71122", "71129", "74909"],
        "benchmark_category": "professional_services",
        "market_score": 78,
    },
    {
        "triggers": ["facilities management", "fm", "building services", "property maintenance",
                     "hard fm", "soft fm", "total facilities"],
        "sic_codes": ["81100", "43290", "43390", "81299", "68320"],
        "benchmark_category": "technical_services",
        "market_score": 76,
    },
    {
        "triggers": ["pest control", "pest management", "vermin control", "rodent control",
                     "fumigation", "disinfection"],
        "sic_codes": ["81223", "81291", "81299"],
        "benchmark_category": "technical_services",
        "market_score": 74,
    },
    {
        "triggers": ["cleaning service", "commercial cleaning", "industrial cleaning",
                     "deep clean", "specialist cleaning", "window cleaning"],
        "sic_codes": ["81210", "81221", "81222", "81299"],
        "benchmark_category": "construction_trades",
        "market_score": 65,
    },
    {
        "triggers": ["freight", "haulage", "road transport", "logistics", "courier",
                     "distribution", "warehousing", "3pl", "supply chain"],
        "sic_codes": ["49410", "52100", "52290", "49390"],
        "benchmark_category": "logistics_transport",
        "market_score": 70,
    },
    {
        "triggers": ["legal", "solicitor", "law firm", "conveyancing", "legal service",
                     "barrister", "legal advice"],
        "sic_codes": ["69101", "69102", "69109"],
        "benchmark_category": "professional_services",
        "market_score": 72,
    },
    {
        "triggers": ["accountancy", "accounting", "audit", "bookkeeping", "tax advisory",
                     "financial reporting", "accountant"],
        "sic_codes": ["69201", "69202", "69203"],
        "benchmark_category": "professional_services",
        "market_score": 70,
    },
    {
        "triggers": ["management consultancy", "business consulting", "strategy consulting",
                     "operations consulting", "management advisory"],
        "sic_codes": ["70229", "70221", "74909"],
        "benchmark_category": "professional_services",
        "market_score": 75,
    },
    {
        "triggers": ["dental", "dentist", "dental practice", "dental laboratory",
                     "orthodontic", "cosmetic dental"],
        "sic_codes": ["86230"],
        "benchmark_category": "healthcare",
        "market_score": 76,
    },
    {
        "triggers": ["care home", "residential care", "nursing home", "elderly care",
                     "domiciliary care", "home care", "supported living"],
        "sic_codes": ["87100", "87300", "88100", "87900"],
        "benchmark_category": "healthcare",
        "market_score": 78,
    },
    {
        "triggers": ["pharmaceutical", "pharma", "drug manufacturer", "clinical research",
                     "life science", "biotech", "medical device"],
        "sic_codes": ["21100", "21200", "72110", "72190", "26600"],
        "benchmark_category": "manufacturing",
        "market_score": 82,
    },
    {
        "triggers": ["food manufacturing", "food production", "food processing",
                     "bakery", "meat processing", "ready meal"],
        "sic_codes": ["10110", "10390", "10710", "10850", "10890"],
        "benchmark_category": "manufacturing",
        "market_score": 70,
    },
    {
        "triggers": ["renewable energy", "solar", "wind energy", "battery storage",
                     "energy storage", "green energy", "clean energy"],
        "sic_codes": ["35110", "35120", "43210", "71122"],
        "benchmark_category": "technical_services",
        "market_score": 86,
    },
    {
        "triggers": ["printing", "commercial print", "print management", "wide format",
                     "packaging print", "label print"],
        "sic_codes": ["18121", "18129", "18130", "17210"],
        "benchmark_category": "manufacturing",
        "market_score": 68,
    },
    {
        "triggers": ["recruitment", "staffing", "employment agency", "temporary staffing",
                     "talent acquisition", "executive search", "headhunt"],
        "sic_codes": ["78101", "78109", "78200", "78300"],
        "benchmark_category": "professional_services",
        "market_score": 70,
    },
    {
        "triggers": ["training", "learning development", "e-learning", "corporate training",
                     "vocational training", "apprenticeship", "skills training"],
        "sic_codes": ["85590", "85510", "85600", "85320"],
        "benchmark_category": "professional_services",
        "market_score": 74,
    },
    {
        "triggers": ["landscape", "grounds maintenance", "horticulture", "garden service",
                     "tree surgery", "arboriculture"],
        "sic_codes": ["81300", "01610", "02400"],
        "benchmark_category": "construction_trades",
        "market_score": 65,
    },
    {
        "triggers": ["water treatment", "water hygiene", "legionella", "water quality",
                     "water management", "water testing"],
        "sic_codes": ["36000", "71200", "74909", "39000"],
        "benchmark_category": "technical_services",
        "market_score": 80,
    },
    {
        "triggers": ["asbestos", "asbestos removal", "asbestos survey", "asbestos management",
                     "asbestos testing", "air monitoring"],
        "sic_codes": ["43110", "71200", "39000", "74901"],
        "benchmark_category": "technical_services",
        "market_score": 82,
    },
    {
        "triggers": ["property survey", "building survey", "chartered surveyor", "quantity surveyor",
                     "structural survey", "valuation"],
        "sic_codes": ["74902", "68310", "71111", "71122"],
        "benchmark_category": "professional_services",
        "market_score": 72,
    },
    {
        "triggers": ["medical equipment", "medical device", "healthcare technology",
                     "medical supplies", "hospital equipment"],
        "sic_codes": ["32500", "26600", "46460"],
        "benchmark_category": "manufacturing",
        "market_score": 82,
    },
    {
        "triggers": ["telecoms", "telecommunications", "broadband", "fibre install",
                     "network infrastructure", "wifi", "structured cabling"],
        "sic_codes": ["61100", "61200", "61900", "43210", "27310"],
        "benchmark_category": "it_digital",
        "market_score": 78,
    },
    {
        "triggers": ["veterinary", "vet", "animal health", "pet care", "veterinary practice"],
        "sic_codes": ["75000", "01621"],
        "benchmark_category": "healthcare",
        "market_score": 74,
    },
    {
        "triggers": ["funeral", "funeral director", "cremation", "bereavement service"],
        "sic_codes": ["96030"],
        "benchmark_category": "professional_services",
        "market_score": 72,
    },
    {
        "triggers": ["automotive", "vehicle repair", "bodyshop", "mot", "garage",
                     "vehicle maintenance", "car repair", "commercial vehicle"],
        "sic_codes": ["45200", "45310", "45400", "33170"],
        "benchmark_category": "technical_services",
        "market_score": 68,
    },
    {
        "triggers": ["demolition", "strip out", "site clearance", "dismantling"],
        "sic_codes": ["43110", "38310", "43999"],
        "benchmark_category": "construction_trades",
        "market_score": 70,
    },
    {
        "triggers": ["occupational health", "health surveillance", "workplace health",
                     "wellbeing service", "OH service"],
        "sic_codes": ["86900", "74909", "86210"],
        "benchmark_category": "healthcare",
        "market_score": 76,
    },
]


# ── Full Companies House condensed SIC code list ───────────────────────────────
# Source: Companies House / ONS SIC 2007 condensed list
SIC_CODES_LIST = {
    # Section A — Agriculture, Forestry and Fishing
    "01110": "Growing of cereals (except rice), leguminous crops and oil seeds",
    "01120": "Growing of rice",
    "01130": "Growing of vegetables and melons, roots and tubers",
    "01150": "Growing of tobacco",
    "01190": "Growing of other non-perennial crops",
    "01210": "Growing of grapes",
    "01220": "Growing of tropical and subtropical fruits",
    "01230": "Growing of citrus fruits",
    "01240": "Growing of pome fruits and stone fruits",
    "01250": "Growing of other tree and bush fruits and nuts",
    "01270": "Growing of beverage crops",
    "01280": "Growing of spices, aromatic, drug and pharmaceutical crops",
    "01290": "Growing of other perennial crops",
    "01300": "Plant propagation",
    "01410": "Raising of dairy cattle",
    "01420": "Raising of other cattle and buffaloes",
    "01430": "Raising of horses and other equines",
    "01450": "Raising of sheep and goats",
    "01460": "Raising of swine/pigs",
    "01470": "Raising of poultry",
    "01490": "Raising of other animals",
    "01500": "Mixed farming",
    "01610": "Support activities for crop production",
    "01621": "Farm animal boarding and care",
    "01629": "Support activities for animal production",
    "01630": "Post-harvest crop activities",
    "01640": "Seed processing for propagation",
    "01700": "Hunting, trapping and related service activities",
    "02100": "Silviculture and other forestry activities",
    "02200": "Logging",
    "02300": "Gathering of wild growing non-wood products",
    "02400": "Support services to forestry",
    "03110": "Marine fishing",
    "03120": "Freshwater fishing",
    "03210": "Marine aquaculture",
    "03220": "Freshwater aquaculture",
    # Section B — Mining and Quarrying
    "05101": "Deep coal mines",
    "05102": "Open cast coal working",
    "06100": "Extraction of crude petroleum",
    "06200": "Extraction of natural gas",
    "07100": "Mining of iron ores",
    "08110": "Quarrying of ornamental and building stone, limestone, gypsum, chalk and slate",
    "08120": "Operation of gravel and sand pits; mining of clays and kaolin",
    "08910": "Mining of chemical and fertiliser minerals",
    "08990": "Other mining and quarrying",
    "09100": "Support activities for petroleum and natural gas extraction",
    "09900": "Support activities for other mining and quarrying",
    # Section C — Manufacturing
    "10110": "Processing and preserving of meat",
    "10120": "Processing and preserving of poultry meat",
    "10130": "Production of meat and poultry meat products",
    "10200": "Processing and preserving of fish, crustaceans and molluscs",
    "10310": "Processing and preserving of potatoes",
    "10320": "Manufacture of fruit and vegetable juice",
    "10390": "Other processing and preserving of fruit and vegetables",
    "10410": "Manufacture of oils and fats",
    "10510": "Liquid milk and cream production; manufacture of milk products",
    "10610": "Grain milling and manufacture of starch products",
    "10710": "Manufacture of bread; fresh pastry goods and cakes",
    "10720": "Manufacture of rusks and biscuits; preserved pastry goods and cakes",
    "10810": "Manufacture of sugar",
    "10821": "Manufacture of cocoa and chocolate confectionery",
    "10850": "Manufacture of prepared meals and dishes",
    "10890": "Manufacture of other food products not elsewhere classified",
    "10910": "Manufacture of prepared feeds for farm animals",
    "10920": "Manufacture of prepared pet foods",
    "11010": "Distilling, rectifying and blending of spirits",
    "11020": "Manufacture of wine from grape",
    "11050": "Manufacture of beer",
    "11070": "Manufacture of soft drinks; production of mineral waters",
    "12000": "Manufacture of tobacco products",
    "13100": "Preparation and spinning of textile fibres",
    "13200": "Weaving of textiles",
    "13300": "Finishing of textiles",
    "13920": "Manufacture of made-up textile articles, except apparel",
    "14110": "Manufacture of leather clothes",
    "14120": "Manufacture of workwear",
    "15110": "Tanning and dressing of leather",
    "15200": "Manufacture of footwear",
    "16100": "Sawmilling and planing of wood",
    "16210": "Manufacture of veneer sheets and wood-based panels",
    "16230": "Manufacture of builders carpentry and joinery",
    "16290": "Manufacture of other products of wood, cork, straw and plaiting materials",
    "17110": "Manufacture of pulp",
    "17120": "Manufacture of paper and paperboard",
    "17210": "Manufacture of corrugated paper and paperboard, sacks and bags",
    "17290": "Manufacture of other articles of paper and paperboard",
    "18110": "Printing of newspapers",
    "18121": "Manufacture of printed labels",
    "18129": "Printing (other than newspapers and labels)",
    "18130": "Pre-press and pre-media services",
    "18140": "Binding and related services",
    "19100": "Manufacture of coke oven products",
    "19201": "Mineral oil refining",
    "20110": "Manufacture of industrial gases",
    "20130": "Manufacture of other inorganic basic chemicals",
    "20140": "Manufacture of other organic basic chemicals",
    "20150": "Manufacture of fertilisers and nitrogen compounds",
    "20160": "Manufacture of plastics in primary forms",
    "20200": "Manufacture of pesticides and other agrochemical products",
    "20301": "Manufacture of paints, varnishes and similar coatings, mastics and sealants",
    "20410": "Manufacture of soap and detergents, cleaning and polishing preparations",
    "20420": "Manufacture of perfumes and toilet preparations",
    "20510": "Manufacture of explosives",
    "20520": "Manufacture of glues",
    "20590": "Manufacture of other chemical products not elsewhere classified",
    "21100": "Manufacture of basic pharmaceutical products",
    "21200": "Manufacture of pharmaceutical preparations",
    "22110": "Manufacture of rubber tyres and tubes",
    "22190": "Manufacture of other rubber products",
    "22210": "Manufacture of plastic plates, sheets, tubes and profiles",
    "22220": "Manufacture of plastic packing goods",
    "22290": "Manufacture of other plastic products",
    "23110": "Manufacture of flat glass",
    "23130": "Manufacture of hollow glass",
    "23310": "Manufacture of ceramic tiles and flags",
    "23320": "Manufacture of bricks, tiles and construction products, in baked clay",
    "23510": "Manufacture of cement",
    "23610": "Manufacture of concrete products for construction purposes",
    "23630": "Manufacture of ready-mixed concrete",
    "23700": "Cutting, shaping and finishing of stone",
    "23910": "Production of abrasive products",
    "24100": "Manufacture of basic iron and steel and of ferro-alloys",
    "24200": "Manufacture of tubes, pipes, hollow profiles and related fittings, of steel",
    "24420": "Aluminium production",
    "24510": "Casting of iron",
    "24520": "Casting of steel",
    "25110": "Manufacture of metal structures and parts of structures",
    "25120": "Manufacture of doors and windows of metal",
    "25210": "Manufacture of central heating radiators and boilers",
    "25290": "Manufacture of other tanks, reservoirs and containers of metal",
    "25400": "Manufacture of weapons and ammunition",
    "25500": "Forging, pressing, stamping and roll-forming of metal; powder metallurgy",
    "25610": "Treatment and coating of metals",
    "25620": "Machining",
    "25730": "Manufacture of tools",
    "25990": "Manufacture of other fabricated metal products not elsewhere classified",
    "26110": "Manufacture of electronic components",
    "26120": "Manufacture of loaded electronic boards",
    "26200": "Manufacture of computers and peripheral equipment",
    "26300": "Manufacture of communication equipment",
    "26400": "Manufacture of consumer electronics",
    "26511": "Manufacture of electronic measuring, testing and controlling equipment",
    "26512": "Manufacture of electronic industrial process control equipment",
    "26513": "Manufacture of non-electronic measuring, testing and controlling equipment",
    "26514": "Manufacture of non-electronic industrial process control equipment",
    "26520": "Manufacture of watches and clocks",
    "26600": "Manufacture of irradiation, electromedical and electrotherapeutic equipment",
    "26701": "Manufacture of optical precision instruments",
    "26702": "Manufacture of photographic and cinematographic equipment",
    "27110": "Manufacture of electric motors, generators and transformers",
    "27120": "Manufacture of electricity distribution and control apparatus",
    "27200": "Manufacture of batteries and accumulators",
    "27310": "Manufacture of fibre optic cables",
    "27320": "Manufacture of other electronic and electric wires and cables",
    "27330": "Manufacture of wiring devices",
    "27400": "Manufacture of electric lighting equipment",
    "27510": "Manufacture of electric domestic appliances",
    "27900": "Manufacture of other electrical equipment",
    "28110": "Manufacture of engines and turbines",
    "28120": "Manufacture of fluid power equipment",
    "28130": "Manufacture of other pumps and compressors",
    "28140": "Manufacture of other taps and valves",
    "28150": "Manufacture of bearings, gears, gearing and driving elements",
    "28220": "Manufacture of lifting and handling equipment",
    "28230": "Manufacture of office machinery and equipment",
    "28240": "Manufacture of power-driven hand tools",
    "28250": "Manufacture of non-domestic cooling and ventilation equipment",
    "28290": "Manufacture of other general-purpose machinery not elsewhere classified",
    "28300": "Manufacture of agricultural and forestry machinery",
    "28920": "Manufacture of machinery for mining, quarrying and construction",
    "28930": "Manufacture of machinery for food, beverage and tobacco processing",
    "28990": "Manufacture of other special-purpose machinery not elsewhere classified",
    "29100": "Manufacture of motor vehicles",
    "29200": "Manufacture of bodies and coachwork for motor vehicles; trailers",
    "29310": "Manufacture of electrical and electronic equipment for motor vehicles",
    "29320": "Manufacture of other parts and accessories for motor vehicles",
    "30110": "Building of ships and floating structures",
    "30120": "Building of pleasure and sporting boats",
    "30200": "Manufacture of railway locomotives and rolling stock",
    "30300": "Manufacture of air and spacecraft and related machinery",
    "30910": "Manufacture of motorcycles",
    "30920": "Manufacture of bicycles and invalid carriages",
    "31010": "Manufacture of office and shop furniture",
    "31020": "Manufacture of kitchen furniture",
    "31090": "Manufacture of other furniture",
    "32120": "Manufacture of jewellery and related articles",
    "32200": "Manufacture of musical instruments",
    "32300": "Manufacture of sports goods",
    "32400": "Manufacture of games and toys",
    "32500": "Manufacture of medical and dental instruments and supplies",
    "32990": "Other manufacturing not elsewhere classified",
    "33110": "Repair of fabricated metal products",
    "33120": "Repair of machinery",
    "33130": "Repair of electronic and optical equipment",
    "33140": "Repair of electrical equipment",
    "33150": "Repair and maintenance of ships and boats",
    "33160": "Repair and maintenance of aircraft and spacecraft",
    "33170": "Repair and maintenance of other transport equipment",
    "33190": "Repair of other equipment",
    "33200": "Installation of industrial machinery and equipment",
    # Section D — Electricity, Gas, Steam and Air Conditioning Supply
    "35110": "Production of electricity",
    "35120": "Transmission of electricity",
    "35130": "Distribution of electricity",
    "35140": "Trade of electricity",
    "35210": "Manufacture of gas",
    "35220": "Distribution of gaseous fuels through mains",
    "35300": "Steam and air conditioning supply",
    # Section E — Water Supply, Sewerage, Waste Management
    "36000": "Water collection, treatment and supply",
    "37000": "Sewerage",
    "38110": "Collection of non-hazardous waste",
    "38120": "Collection of hazardous waste",
    "38210": "Treatment and disposal of non-hazardous waste",
    "38220": "Treatment and disposal of hazardous waste",
    "38310": "Dismantling of wrecks",
    "38320": "Recovery of sorted materials",
    "39000": "Remediation activities and other waste management services",
    # Section F — Construction
    "41100": "Development of building projects",
    "41201": "Construction of commercial buildings",
    "41202": "Construction of domestic buildings",
    "42110": "Construction of roads and motorways",
    "42120": "Construction of railways and underground railways",
    "42130": "Construction of bridges and tunnels",
    "42210": "Construction of utility projects for fluids",
    "42220": "Construction of utility projects for electricity and telecommunications",
    "42910": "Construction of water projects",
    "42990": "Construction of other civil engineering projects",
    "43110": "Demolition",
    "43120": "Site preparation",
    "43130": "Test drilling and boring",
    "43210": "Electrical installation",
    "43220": "Plumbing, heat and air-conditioning installation",
    "43290": "Other construction installation",
    "43310": "Plastering",
    "43320": "Joinery installation",
    "43330": "Floor and wall covering",
    "43340": "Painting and glazing",
    "43341": "Painting",
    "43342": "Glazing",
    "43390": "Other building completion and finishing",
    "43910": "Roofing activities",
    "43991": "Scaffold erection",
    "43999": "Other specialised construction activities",
    # Section G — Wholesale and Retail Trade
    "45200": "Maintenance and repair of motor vehicles",
    "45310": "Wholesale trade of motor vehicle parts and accessories",
    "46110": "Agents selling agricultural raw materials, livestock, textile raw materials",
    "46620": "Wholesale of machine tools",
    "46630": "Wholesale of mining, construction and civil engineering machinery",
    "46640": "Wholesale of machinery for the textile industry",
    "46650": "Wholesale of office furniture",
    "46660": "Wholesale of other office machinery and equipment",
    "46690": "Wholesale of other machinery and equipment",
    "46711": "Wholesale of petroleum and petroleum products",
    "46720": "Wholesale of metals and metal ores",
    "46730": "Wholesale of wood, construction materials and sanitary equipment",
    "46740": "Wholesale of hardware, plumbing and heating equipment and supplies",
    "46750": "Wholesale of chemical products",
    "46770": "Wholesale of waste and scrap",
    "46460": "Wholesale of pharmaceutical goods",
    "47410": "Retail sale of computers, peripheral units and software",
    "47520": "Retail sale of hardware, paints and glass",
    "47540": "Retail sale of electrical household appliances",
    "47730": "Dispensing chemist in specialised stores",
    "47782": "Retail sale by opticians",
    # Section H — Transportation and Storage
    "49100": "Passenger rail transport, interurban",
    "49200": "Freight rail transport",
    "49320": "Taxi operation",
    "49390": "Other passenger land transport",
    "49410": "Freight transport by road",
    "49420": "Removal services",
    "49500": "Transport via pipeline",
    "52100": "Warehousing and storage",
    "52210": "Service activities incidental to land transportation",
    "52290": "Other transportation support activities",
    "53100": "Postal activities under universal service obligation",
    "53201": "Licensed carriers",
    "53202": "Unlicensed carriers",
    # Section I — Accommodation and Food Service
    "55100": "Hotels and similar accommodation",
    "56101": "Licenced restaurants",
    "56102": "Unlicenced restaurants and cafes",
    "56210": "Event catering activities",
    "56290": "Other food services",
    "56301": "Licenced clubs",
    "56302": "Public houses and bars",
    # Section J — Information and Communication
    "58110": "Book publishing",
    "58190": "Other publishing activities",
    "58210": "Publishing of computer games",
    "58290": "Other software publishing",
    "59111": "Motion picture production activities",
    "59200": "Sound recording and music publishing activities",
    "60100": "Radio broadcasting",
    "60200": "Television programming and broadcasting activities",
    "61100": "Wired telecommunications activities",
    "61200": "Wireless telecommunications activities",
    "61300": "Satellite telecommunications activities",
    "61900": "Other telecommunications activities",
    "62011": "Ready-made interactive leisure and entertainment software development",
    "62012": "Business and domestic software development",
    "62020": "Information technology consultancy activities",
    "62030": "Computer facilities management activities",
    "62090": "Other information technology service activities",
    "63110": "Data processing, hosting and related activities",
    "63120": "Web portals",
    "63910": "News agency activities",
    "63990": "Other information service activities",
    # Section K — Financial and Insurance Activities
    "64191": "Banks",
    "64303": "Activities of venture and development capital companies",
    "64910": "Financial leasing",
    "64921": "Credit granting by non-deposit taking finance houses",
    "64929": "Other credit granting not elsewhere classified",
    "64999": "Other financial service activities",
    "65110": "Life insurance",
    "65120": "Non-life insurance",
    "65300": "Pension funding",
    "66110": "Administration of financial markets",
    "66120": "Security and commodity contracts dealing activities",
    "66190": "Other activities auxiliary to financial services",
    "66210": "Risk and damage evaluation",
    "66220": "Activities of insurance agents and brokers",
    "66290": "Other activities auxiliary to insurance and pension funding",
    "66300": "Fund management activities",
    # Section L — Real Estate
    "68100": "Buying and selling of own real estate",
    "68201": "Renting and operating of Housing Association real estate",
    "68209": "Other letting and operating of own or leased real estate",
    "68310": "Real estate agencies",
    "68320": "Management of real estate on a fee or contract basis",
    # Section M — Professional, Scientific and Technical Activities
    "69101": "Barristers at law",
    "69102": "Solicitors",
    "69109": "Activities of patent and copyright agents; other legal activities",
    "69201": "Accounting and auditing activities",
    "69202": "Bookkeeping activities",
    "69203": "Tax consultancy",
    "70100": "Activities of head offices",
    "70210": "Public relations and communications activities",
    "70221": "Financial management",
    "70229": "Management consultancy activities",
    "71111": "Architectural activities",
    "71112": "Urban planning and landscape architectural activities",
    "71121": "Engineering design activities for industrial process and production",
    "71122": "Engineering related scientific and technical consulting activities",
    "71129": "Other engineering activities",
    "71200": "Technical testing and analysis",
    "72110": "Research and experimental development on biotechnology",
    "72190": "Other research and experimental development on natural sciences",
    "72200": "Research and experimental development on social sciences",
    "73110": "Advertising agencies",
    "73200": "Market research and public opinion polling",
    "74100": "Specialised design activities",
    "74201": "Portrait photographic activities",
    "74202": "Other specialist photography",
    "74300": "Translation and interpretation activities",
    "74901": "Environmental consulting activities",
    "74902": "Quantity surveying activities",
    "74909": "Other professional, scientific and technical activities",
    "75000": "Veterinary activities",
    # Section N — Administrative and Support Service Activities
    "77110": "Renting and leasing of cars and light motor vehicles",
    "77120": "Renting and leasing of trucks and other heavy vehicles",
    "77310": "Renting and leasing of agricultural machinery and equipment",
    "77320": "Renting and leasing of construction and civil engineering machinery",
    "77330": "Renting and leasing of office machinery and equipment",
    "77390": "Renting and leasing of other machinery, equipment and tangible goods",
    "78101": "Motion picture, television and other theatrical casting",
    "78109": "Other activities of employment placement agencies",
    "78200": "Temporary employment agency activities",
    "78300": "Human resources provision and management of human resources functions",
    "79110": "Travel agency activities",
    "79120": "Tour operator activities",
    "80100": "Private security activities",
    "80200": "Security systems service activities",
    "80300": "Investigation activities",
    "81100": "Combined facilities support activities",
    "81210": "General cleaning of buildings",
    "81221": "Window cleaning services",
    "81222": "Specialised cleaning services",
    "81223": "Fumigation and pest control services",
    "81291": "Disinfecting and exterminating services",
    "81299": "Other building and industrial cleaning activities",
    "81300": "Landscape service activities",
    "82110": "Combined office administrative service activities",
    "82190": "Photocopying, document preparation and other office support activities",
    "82200": "Activities of call centres",
    "82301": "Activities of exhibition and fair organisers",
    "82302": "Activities of conference organisers",
    "82911": "Activities of collection agencies",
    "82920": "Packaging activities",
    "82990": "Other business support service activities",
    # Section O — Public Administration
    "84110": "General public administration activities",
    "84240": "Public order and safety activities",
    "84250": "Fire service activities",
    # Section P — Education
    "85100": "Pre-primary education",
    "85200": "Primary education",
    "85310": "General secondary education",
    "85320": "Technical and vocational secondary education",
    "85421": "First-degree level higher education",
    "85510": "Sports and recreation education",
    "85520": "Cultural education",
    "85530": "Driving school activities",
    "85590": "Other education not elsewhere classified",
    "85600": "Educational support activities",
    # Section Q — Human Health and Social Work
    "86101": "Hospital activities",
    "86102": "Medical nursing home activities",
    "86210": "General medical practice activities",
    "86220": "Specialists medical practice activities",
    "86230": "Dental practice activities",
    "86900": "Other human health activities",
    "87100": "Residential nursing care activities",
    "87200": "Residential care activities for learning difficulties, mental health and substance abuse",
    "87300": "Residential care activities for the elderly and disabled",
    "87900": "Other residential care activities",
    "88100": "Social work activities without accommodation for the elderly and disabled",
    "88910": "Child day-care activities",
    "88990": "Other social work activities without accommodation",
    # Section R — Arts, Entertainment and Recreation
    "90010": "Performing arts",
    "90030": "Artistic creation",
    "91020": "Museum activities",
    "92000": "Gambling and betting activities",
    "93110": "Operation of sports facilities",
    "93120": "Activities of sport clubs",
    "93130": "Fitness facilities",
    "93190": "Other sports activities",
    "93210": "Activities of amusement parks and theme parks",
    "93290": "Other amusement and recreation activities",
    # Section S — Other Service Activities
    "94110": "Activities of business and employers membership organisations",
    "94120": "Activities of professional membership organisations",
    "94200": "Activities of trade unions",
    "95110": "Repair of computers and peripheral equipment",
    "95120": "Repair of communication equipment",
    "95210": "Repair of consumer electronics",
    "95220": "Repair of household appliances and home and garden equipment",
    "95290": "Repair of personal and household goods not elsewhere classified",
    "96010": "Washing and (dry-)cleaning of textile and fur products",
    "96020": "Hairdressing and other beauty treatment",
    "96030": "Funeral and related activities",
    "96040": "Physical well-being activities",
    "96090": "Other service activities not elsewhere classified",
}


# ── Sector benchmark defaults by broad category ───────────────────────────────
BENCHMARK_DEFAULTS = {
    "professional_services": {
        "revenue_per_head_low": 60_000,
        "revenue_per_head_base": 85_000,
        "revenue_per_head_high": 120_000,
        "asset_turnover_ratio": 2.5,
        "revenue_per_site": 500_000,
        "ebitda_margin_low": 0.12,
        "ebitda_margin_base": 0.18,
        "ebitda_margin_high": 0.25,
        "estimated_market_size_gbp": 1_000_000_000,
        "estimated_top5_market_share": 0.20,
        "sector_b2b_score": 90,
    },
    "technical_services": {
        "revenue_per_head_low": 65_000,
        "revenue_per_head_base": 85_000,
        "revenue_per_head_high": 110_000,
        "asset_turnover_ratio": 2.0,
        "revenue_per_site": 700_000,
        "ebitda_margin_low": 0.10,
        "ebitda_margin_base": 0.16,
        "ebitda_margin_high": 0.22,
        "estimated_market_size_gbp": 2_000_000_000,
        "estimated_top5_market_share": 0.25,
        "sector_b2b_score": 92,
    },
    "construction_trades": {
        "revenue_per_head_low": 50_000,
        "revenue_per_head_base": 70_000,
        "revenue_per_head_high": 90_000,
        "asset_turnover_ratio": 1.8,
        "revenue_per_site": 600_000,
        "ebitda_margin_low": 0.07,
        "ebitda_margin_base": 0.12,
        "ebitda_margin_high": 0.18,
        "estimated_market_size_gbp": 5_000_000_000,
        "estimated_top5_market_share": 0.15,
        "sector_b2b_score": 80,
    },
    "manufacturing": {
        "revenue_per_head_low": 80_000,
        "revenue_per_head_base": 110_000,
        "revenue_per_head_high": 150_000,
        "asset_turnover_ratio": 1.2,
        "revenue_per_site": 2_000_000,
        "ebitda_margin_low": 0.08,
        "ebitda_margin_base": 0.13,
        "ebitda_margin_high": 0.20,
        "estimated_market_size_gbp": 3_000_000_000,
        "estimated_top5_market_share": 0.30,
        "sector_b2b_score": 85,
    },
    "waste_environmental": {
        "revenue_per_head_low": 55_000,
        "revenue_per_head_base": 75_000,
        "revenue_per_head_high": 95_000,
        "asset_turnover_ratio": 1.5,
        "revenue_per_site": 1_000_000,
        "ebitda_margin_low": 0.10,
        "ebitda_margin_base": 0.15,
        "ebitda_margin_high": 0.22,
        "estimated_market_size_gbp": 4_000_000_000,
        "estimated_top5_market_share": 0.20,
        "sector_b2b_score": 88,
    },
    "it_digital": {
        "revenue_per_head_low": 70_000,
        "revenue_per_head_base": 100_000,
        "revenue_per_head_high": 140_000,
        "asset_turnover_ratio": 3.0,
        "revenue_per_site": 800_000,
        "ebitda_margin_low": 0.12,
        "ebitda_margin_base": 0.20,
        "ebitda_margin_high": 0.30,
        "estimated_market_size_gbp": 10_000_000_000,
        "estimated_top5_market_share": 0.20,
        "sector_b2b_score": 85,
    },
    "healthcare": {
        "revenue_per_head_low": 40_000,
        "revenue_per_head_base": 55_000,
        "revenue_per_head_high": 75_000,
        "asset_turnover_ratio": 1.5,
        "revenue_per_site": 1_500_000,
        "ebitda_margin_low": 0.08,
        "ebitda_margin_base": 0.14,
        "ebitda_margin_high": 0.20,
        "estimated_market_size_gbp": 5_000_000_000,
        "estimated_top5_market_share": 0.25,
        "sector_b2b_score": 60,
    },
    "logistics_transport": {
        "revenue_per_head_low": 45_000,
        "revenue_per_head_base": 65_000,
        "revenue_per_head_high": 85_000,
        "asset_turnover_ratio": 2.2,
        "revenue_per_site": 800_000,
        "ebitda_margin_low": 0.06,
        "ebitda_margin_base": 0.10,
        "ebitda_margin_high": 0.16,
        "estimated_market_size_gbp": 8_000_000_000,
        "estimated_top5_market_share": 0.20,
        "sector_b2b_score": 75,
    },
    "default": {
        "revenue_per_head_low": 55_000,
        "revenue_per_head_base": 75_000,
        "revenue_per_head_high": 100_000,
        "asset_turnover_ratio": 2.0,
        "revenue_per_site": 700_000,
        "ebitda_margin_low": 0.09,
        "ebitda_margin_base": 0.14,
        "ebitda_margin_high": 0.20,
        "estimated_market_size_gbp": 2_000_000_000,
        "estimated_top5_market_share": 0.20,
        "sector_b2b_score": 80,
    },
}

# SIC prefix → benchmark category
SIC_BENCHMARK_MAP = {
    "41": "construction_trades", "42": "construction_trades", "43": "construction_trades",
    "10": "manufacturing",       "11": "manufacturing",       "12": "manufacturing",
    "13": "manufacturing",       "14": "manufacturing",       "15": "manufacturing",
    "16": "manufacturing",       "17": "manufacturing",       "18": "manufacturing",
    "19": "manufacturing",       "20": "manufacturing",       "21": "manufacturing",
    "22": "manufacturing",       "23": "manufacturing",       "24": "manufacturing",
    "25": "manufacturing",       "26": "manufacturing",       "27": "manufacturing",
    "28": "manufacturing",       "29": "manufacturing",       "30": "manufacturing",
    "31": "manufacturing",       "32": "manufacturing",       "33": "manufacturing",
    "38": "waste_environmental",  "39": "waste_environmental",
    "36": "waste_environmental",  "37": "waste_environmental",
    "49": "logistics_transport",  "50": "logistics_transport",
    "51": "logistics_transport",  "52": "logistics_transport",
    "53": "logistics_transport",
    "62": "it_digital",           "63": "it_digital",
    "58": "it_digital",           "59": "it_digital",          "60": "it_digital",
    "61": "it_digital",
    "86": "healthcare",           "87": "healthcare",          "88": "healthcare",
    "71": "technical_services",   "72": "technical_services",
    "69": "professional_services", "70": "professional_services",
    "73": "professional_services", "74": "professional_services",
}


def _curated_match(sector_description: str) -> dict | None:
    """
    Check user description against the curated sector map.
    Returns the best matching entry, or None if no strong match found.
    Scores by counting how many trigger phrases are present in the description.
    """
    desc_lower = sector_description.lower()
    best       = None
    best_score = 0

    for entry in CURATED_SECTORS:
        score = 0
        for trigger in entry["triggers"]:
            if trigger in desc_lower:
                # Longer triggers score higher (more specific)
                score += len(trigger.split())

        if score > best_score:
            best_score = score
            best = entry

    # Only return if we have a meaningful match (at least 1 trigger word)
    return best if best_score >= 1 else None


def _fuzzy_sic_match(sector_description: str, top_n: int = 8, min_score: float = 0.15) -> list[dict]:
    """
    Fallback: score all SIC codes against the description using keyword overlap + fuzzy.
    Returns top_n matches with score >= min_score.
    """
    desc_words = set(re.findall(r"\b\w{4,}\b", sector_description.lower()))
    results    = []

    for code, sic_desc in SIC_CODES_LIST.items():
        sic_words    = set(re.findall(r"\b\w{4,}\b", sic_desc.lower()))
        overlap      = len(desc_words & sic_words)
        overlap_score = min(overlap / max(len(desc_words), 1), 1.0)
        fuzzy_score  = SequenceMatcher(None, sector_description.lower(), sic_desc.lower()).ratio()
        score        = (overlap_score * 0.75) + (fuzzy_score * 0.25)

        if score >= min_score:
            results.append({"code": code, "description": sic_desc, "score": round(score, 3)})

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_n]


def _extract_keywords(sector_description: str) -> dict:
    """Extract include_stems, name_queries and exclude_terms from the sector description."""
    stop_words = {
        "that", "this", "with", "from", "have", "been", "will", "they", "their",
        "also", "into", "only", "some", "very", "more", "most", "than", "then",
        "when", "where", "which", "while", "about", "after", "before", "other",
        "these", "those", "would", "could", "should", "businesses", "companies",
        "company", "sector", "industry", "services", "service", "activities",
        "activity", "united", "kingdom", "england", "scotland", "wales",
    }
    words       = re.findall(r"\b\w{4,}\b", sector_description.lower())
    meaningful  = [w for w in words if w not in stop_words]

    # Deduplicate on 5-char prefix, keep 6-char stems
    seen, stems = set(), []
    for w in meaningful:
        pfx = w[:5]
        if pfx not in seen:
            seen.add(pfx)
            stems.append(w[:6])

    # Name queries
    raw_words   = sector_description.lower().split()
    queries     = [sector_description.lower()]
    if len(raw_words) >= 2:
        queries.append(" ".join(raw_words[:2]))
    queries.extend(meaningful[:4])
    queries     = list(dict.fromkeys(queries))[:6]

    # Generic PE B2B exclusions
    exclude_terms = [
        "dental", "dentist", "catering", "restaurant", "hair", "beauty",
        "tattoo", "photography", "photographer", "driving school",
        "nursery", "funeral", "fashion", "cleaning", "recruitment",
        "painting", "landscaping", "letting", "solicitor", "flooring",
        "pathology", "veterinary",
    ]

    return {"include_stems": stems, "name_queries": queries, "exclude_terms": exclude_terms}


def _validate_code_company_count(code: str, api_key: str) -> int:
    """Check how many active companies are registered under a given SIC code."""
    try:
        r = requests.get(
            "https://api.company-information.service.gov.uk/advanced-search/companies",
            params={"sic_codes": code, "company_status": "active", "size": 1},
            auth=(api_key, ""),
            timeout=10,
        )
        if r.status_code == 200:
            return r.json().get("hits", 0)
    except Exception:
        pass
    return 0


def discover(
    sector_description: str,
    api_key:            str | None = None,
    top_sic:            int  = 6,
    min_score:          float = 0.15,
    validate:           bool  = False,
) -> object:
    """
    Main entry point. Returns a config-compatible namespace for any sector description.

    Args:
        sector_description: Free-text sector (e.g. "fire safety", "electrical contractors")
        api_key:            Companies House API key (used for validation)
        top_sic:            Maximum SIC codes to include
        min_score:          Minimum relevance score for fuzzy fallback
        validate:           If True, validate codes against CH API company counts

    Returns:
        SimpleNamespace with all pipeline config fields populated.
    """
    from types import SimpleNamespace

    print(f"\n🔍 Discovering SIC codes for: '{sector_description}' ...")

    # ── Step 0: Direct SIC code input ─────────────────────────────────────────
    # If the user typed a raw SIC code (e.g. "24430") or comma-separated codes,
    # look them up directly instead of fuzzy matching on the text.
    _raw = sector_description.strip().replace(" ", "")
    _raw_codes = [c.strip() for c in _raw.split(",") if c.strip().isdigit()]
    if _raw_codes and all(c.isdigit() for c in _raw_codes):
        # Validate each code exists in the SIC list
        valid_codes = [c for c in _raw_codes if c in SIC_CODES_LIST]
        if valid_codes:
            sic_code_list = valid_codes[:top_sic]
            selected = [
                {
                    "code":        c,
                    "description": SIC_CODES_LIST.get(c, ""),
                    "score":       1.0,
                    "source":      "direct SIC code",
                }
                for c in sic_code_list
            ]
            bench_cat    = _infer_benchmark_category(sic_code_list)
            market_score = min(int(BENCHMARK_DEFAULTS[bench_cat]["sector_b2b_score"] * 0.85), 85)
            source       = "direct SIC code"
            print(f"  ✓ Direct SIC code lookup: {', '.join(sic_code_list)}")

            cfg = SimpleNamespace(
                SIC_CODES          = sic_code_list,
                SECTOR_DESCRIPTION = ", ".join(SIC_CODES_LIST.get(c, c) for c in sic_code_list),
                BENCHMARK_CATEGORY = bench_cat,
                MARKET_SCORE       = market_score,
                EXCLUDE_SUBSECTORS = [],
                _sic_matches       = selected,
                _source            = source,
            )
            print(f"\n📋 Config ready — {len(sic_code_list)} SIC codes via {source}")
            return cfg

    # ── Step 1: Try curated map first ─────────────────────────────────────────
    curated = _curated_match(sector_description)

    if curated:
        sic_code_list    = curated["sic_codes"][:top_sic]
        bench_cat        = curated["benchmark_category"]
        market_score     = curated["market_score"]
        curated_excludes = curated.get("exclude_subsectors", [])
        source           = "curated sector map"
        # Build match metadata for display
        selected = [
            {
                "code":        c,
                "description": SIC_CODES_LIST.get(c, ""),
                "score":       1.0,
                "source":      "curated",
            }
            for c in sic_code_list
        ]
        print(f"  ✓ Matched via curated sector map")
    else:
        curated_excludes = []
        # ── Step 2: Fuzzy fallback ─────────────────────────────────────────────
        print(f"  No curated match found — using fuzzy SIC scoring ...")
        selected = _fuzzy_sic_match(sector_description, top_n=top_sic, min_score=min_score)
        if not selected:
            raise ValueError(
                f"No SIC codes found for '{sector_description}'. "
                "Try a more specific description (e.g. 'electrical installation contractors')."
            )
        sic_code_list = [m["code"] for m in selected]
        bench_cat     = _infer_benchmark_category(sic_code_list)
        market_score  = min(int(BENCHMARK_DEFAULTS[bench_cat]["sector_b2b_score"] * 0.85), 85)
        source        = "fuzzy matching"
        print(f"  ✓ Fuzzy matched {len(selected)} codes")

    # ── Step 3: Optional CH API validation ────────────────────────────────────
    if validate and api_key:
        print("  Validating codes against Companies House API ...")
        for m in selected:
            count = _validate_code_company_count(m["code"], api_key)
            m["company_count"] = count
            time.sleep(0.3)
        # Remove codes with <10 companies (likely irrelevant)
        selected      = [m for m in selected if m.get("company_count", 10) >= 10]
        sic_code_list = [m["code"] for m in selected]
    else:
        for m in selected:
            if "company_count" not in m:
                m["company_count"] = None

    print(f"\n  Source: {source}")
    print(f"  SIC codes selected:")
    for m in selected:
        count_str = f"  ({m['company_count']:,} companies)" if m.get("company_count") else ""
        print(f"    {m['code']}  {m['description']}{count_str}")

    # ── Step 4: Extract keywords ───────────────────────────────────────────────
    kw = _extract_keywords(sector_description)

    # ── Step 5: Benchmarks ────────────────────────────────────────────────────
    benchmarks = BENCHMARK_DEFAULTS[bench_cat].copy()

    # ── Step 6: Bolt-on adjacencies ───────────────────────────────────────────
    bolt_on = _build_bolt_on_adjacencies(sector_description, sic_code_list)

    # ── Step 7: Assemble config ───────────────────────────────────────────────
    label = sector_description.title()

    cfg = SimpleNamespace(
        SECTOR_LABEL            = f"UK {label}",
        SECTOR_DESCRIPTION      = sector_description,
        SIC_CODES               = sic_code_list,
        NAME_QUERIES            = kw["name_queries"],
        INCLUDE_STEMS           = kw["include_stems"],
        EXCLUDE_TERMS           = kw["exclude_terms"],
        EXCLUDE_SUBSECTORS      = curated_excludes,
        SECTOR_BENCHMARKS       = benchmarks,
        REVENUE_PER_HEAD_LOW    = benchmarks["revenue_per_head_low"],
        REVENUE_PER_HEAD_MID    = benchmarks["revenue_per_head_base"],
        REVENUE_PER_HEAD_HIGH   = benchmarks["revenue_per_head_high"],
        ASSET_TURNOVER_RATIO    = benchmarks["asset_turnover_ratio"],
        EBITDA_MARGIN_LOW       = benchmarks["ebitda_margin_low"],
        EBITDA_MARGIN_BASE      = benchmarks["ebitda_margin_base"],
        EBITDA_MARGIN_HIGH      = benchmarks["ebitda_margin_high"],
        TARGET_REVENUE_MIN      = 5_000_000,
        TARGET_REVENUE_MAX      = 30_000_000,
        TARGET_EBITDA_MIN       = 1_000_000,
        TARGET_EBITDA_MAX       = 5_000_000,
        FOUNDER_AGE_FLOOR       = 55,
        SCORE_WEIGHTS           = {
            "scale_financial":       0.30,
            "market_attractiveness": 0.20,
            "ownership_succession":  0.30,
            "dealability_signals":   0.20,
        },
        SCORE_THRESHOLDS        = {"prime": 80, "high": 65, "medium": 50},
        MARKET_ATTRACTIVENESS_SCORE = market_score,
        CONTACT_ENRICH_TOP_N    = 50,
        BOLT_ON_ADJACENCIES     = bolt_on,
        OUTPUT_DIR              = "output",
        RAW_JSON                = "raw_companies.json",
        FILTERED_JSON           = "filtered_companies.json",
        ENRICHED_JSON           = "enriched_companies.json",
        EXCEL_OUTPUT            = "PE_Pipeline.xlsx",
        _sic_matches            = selected,
        _benchmark_category     = bench_cat,
    )

    print(f"\n  Config ready  →  '{cfg.SECTOR_LABEL}'")
    print(f"  Benchmarks    →  {bench_cat}  |  Rev/head: £{benchmarks['revenue_per_head_base']:,}  |  EBITDA: {int(benchmarks['ebitda_margin_base']*100)}%")
    print(f"  Name queries  →  {cfg.NAME_QUERIES}")
    print(f"  Include stems →  {cfg.INCLUDE_STEMS}")

    return cfg


def _infer_benchmark_category(sic_codes: list[str]) -> str:
    for code in sic_codes:
        prefix = code[:2]
        if prefix in SIC_BENCHMARK_MAP:
            return SIC_BENCHMARK_MAP[prefix]
    return "default"


def _build_bolt_on_adjacencies(sector_description: str, sic_codes: list[str]) -> list[dict]:
    """Build sector-relevant bolt-on adjacency suggestions."""
    desc_lower  = sector_description.lower()
    adjacencies = []

    # ── Sector-specific top adjacency ─────────────────────────────────────────
    if any(w in desc_lower for w in ["fire", "flame", "sprinkler", "suppression"]):
        adjacencies.append({
            "cluster": "Fire Risk Assessment & Consulting",
            "rationale": "Higher-margin advisory on top of installation; same buyer, larger wallet",
            "bolt_on_services": ["fire risk assessments", "passive fire protection", "emergency lighting"],
            "sic_codes": ["71122", "74909"],
            "opportunity_score": 9,
        })
    elif any(w in desc_lower for w in ["electrical", "electric", "power", "wiring"]):
        adjacencies.append({
            "cluster": "EV Charging & Renewables Installation",
            "rationale": "Fast-growing adjacent market; existing electrical workforce directly redeployable",
            "bolt_on_services": ["EV charger installation", "solar PV", "battery storage"],
            "sic_codes": ["43210", "35110"],
            "opportunity_score": 9,
        })
    elif any(w in desc_lower for w in ["plumb", "hvac", "heating", "ventilat", "heat pump", "boiler"]):
        adjacencies.append({
            "cluster": "Heat Pump & Renewable Heating",
            "rationale": "Decarbonisation targets driving rapid demand; same workforce",
            "bolt_on_services": ["heat pump installation", "solar thermal", "biomass boilers"],
            "sic_codes": ["43220", "35300"],
            "opportunity_score": 9,
        })
    elif any(w in desc_lower for w in ["waste", "recycl", "disposal", "hazard"]):
        adjacencies.append({
            "cluster": "Environmental Consulting & Remediation",
            "rationale": "Regulatory requirements growing; same client base, higher-margin advisory overlay",
            "bolt_on_services": ["environmental impact assessments", "soil remediation", "contaminated land"],
            "sic_codes": ["74901", "39000"],
            "opportunity_score": 9,
        })
    elif any(w in desc_lower for w in ["security", "cctv", "access control", "surveillance", "alarm"]):
        adjacencies.append({
            "cluster": "Cybersecurity & Remote Monitoring",
            "rationale": "Physical security clients increasingly require integrated cyber/remote services",
            "bolt_on_services": ["remote monitoring centres", "cyber security audits", "cloud access control"],
            "sic_codes": ["80200", "62020"],
            "opportunity_score": 9,
        })
    elif any(w in desc_lower for w in ["it ", "msp", "managed service", "software", "cloud", "cyber", "tech"]):
        adjacencies.append({
            "cluster": "Managed Security Services (MSSP)",
            "rationale": "High-margin recurring revenue; natural extension for IT MSPs",
            "bolt_on_services": ["SOC-as-a-service", "penetration testing", "vulnerability management"],
            "sic_codes": ["80200", "62020"],
            "opportunity_score": 9,
        })
    elif any(w in desc_lower for w in ["calibrat", "metrolog", "testing", "inspection", "ndt", "measurement"]):
        adjacencies.append({
            "cluster": "Dimensional Inspection & NDT",
            "rationale": "Shared customer base; complementary equipment; natural upsell",
            "bolt_on_services": ["3D scanning", "CMM inspection", "ultrasonic NDT", "radiographic testing"],
            "sic_codes": ["71200", "71122"],
            "opportunity_score": 9,
        })

    # ── Universal bolt-ons ─────────────────────────────────────────────────────
    adjacencies.extend([
        {
            "cluster": "Compliance & Certification Services",
            "rationale": "Regulatory burden creates recurring advisory revenue; natural upsell to existing client base",
            "bolt_on_services": ["ISO certification support", "regulatory compliance consulting", "audit preparation"],
            "sic_codes": ["74909", "71122"],
            "opportunity_score": 8,
        },
        {
            "cluster": "Digital & Software Solutions",
            "rationale": "Sector-specific software or IoT creates recurring SaaS revenue and client stickiness",
            "bolt_on_services": ["field management software", "asset tracking", "digital reporting platforms"],
            "sic_codes": ["62020", "62090", "63110"],
            "opportunity_score": 7,
        },
        {
            "cluster": "Training & Accreditation",
            "rationale": "Sector expertise monetised as training; high-margin, scalable, recurring revenue",
            "bolt_on_services": ["staff training", "accreditation programmes", "e-learning content"],
            "sic_codes": ["85590", "85510"],
            "opportunity_score": 7,
        },
    ])

    return adjacencies[:5]


def save_config_file(cfg, output_path: str) -> str:
    """Serialise a discovered config to a Python module file."""
    sic_list_str = "\n".join(
        f'    "{c}",  # {SIC_CODES_LIST.get(c, "")}' for c in cfg.SIC_CODES
    )
    includes_str = "\n".join(f'    "{s}",' for s in cfg.INCLUDE_STEMS)
    queries_str  = "\n".join(f'    "{q}",' for q in cfg.NAME_QUERIES)
    excludes_str = "\n".join(f'    "{e}",' for e in cfg.EXCLUDE_TERMS)
    bolt_on_str  = json.dumps(cfg.BOLT_ON_ADJACENCIES, indent=4)

    content = f'''"""
Auto-generated sector config.
Generated from: "{cfg.SECTOR_DESCRIPTION}"
Re-run: python run.py --sector "{cfg.SECTOR_DESCRIPTION}" --save-config {output_path}
"""

SECTOR_LABEL = "{cfg.SECTOR_LABEL}"

SIC_CODES = [
{sic_list_str}
]

NAME_QUERIES = [
{queries_str}
]

INCLUDE_STEMS = [
{includes_str}
]

EXCLUDE_TERMS = [
{excludes_str}
]

EXCLUDE_SUBSECTORS = []

SECTOR_BENCHMARKS = {json.dumps(cfg.SECTOR_BENCHMARKS, indent=4)}

REVENUE_PER_HEAD_LOW  = SECTOR_BENCHMARKS["revenue_per_head_low"]
REVENUE_PER_HEAD_MID  = SECTOR_BENCHMARKS["revenue_per_head_base"]
REVENUE_PER_HEAD_HIGH = SECTOR_BENCHMARKS["revenue_per_head_high"]
ASSET_TURNOVER_RATIO  = SECTOR_BENCHMARKS["asset_turnover_ratio"]
EBITDA_MARGIN_LOW     = SECTOR_BENCHMARKS["ebitda_margin_low"]
EBITDA_MARGIN_BASE    = SECTOR_BENCHMARKS["ebitda_margin_base"]
EBITDA_MARGIN_HIGH    = SECTOR_BENCHMARKS["ebitda_margin_high"]

TARGET_REVENUE_MIN  = {cfg.TARGET_REVENUE_MIN}
TARGET_REVENUE_MAX  = {cfg.TARGET_REVENUE_MAX}
TARGET_EBITDA_MIN   = {cfg.TARGET_EBITDA_MIN}
TARGET_EBITDA_MAX   = {cfg.TARGET_EBITDA_MAX}
FOUNDER_AGE_FLOOR   = {cfg.FOUNDER_AGE_FLOOR}

SCORE_WEIGHTS = {{
    "scale_financial":       0.30,
    "market_attractiveness": 0.20,
    "ownership_succession":  0.30,
    "dealability_signals":   0.20,
}}

SCORE_THRESHOLDS = {{"prime": 80, "high": 65, "medium": 50}}

MARKET_ATTRACTIVENESS_SCORE = {cfg.MARKET_ATTRACTIVENESS_SCORE}
CONTACT_ENRICH_TOP_N        = {cfg.CONTACT_ENRICH_TOP_N}

BOLT_ON_ADJACENCIES = {bolt_on_str}

OUTPUT_DIR    = "output"
RAW_JSON      = "raw_companies.json"
FILTERED_JSON = "filtered_companies.json"
ENRICHED_JSON = "enriched_companies.json"
EXCEL_OUTPUT  = "PE_Pipeline.xlsx"
'''
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w") as f:
        f.write(content)
    return output_path


if __name__ == "__main__":
    import sys
    desc = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "fire safety and protection systems"
    cfg = discover(desc)
    print("\nFull SIC matches:")
    for m in cfg._sic_matches:
        print(f"  {m['code']}  {m['description']}")
