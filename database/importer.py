from __future__ import annotations

"""Proces importu CSV -> SQLite wraz z czyszczeniem i normalizacją danych."""

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .connection import get_connection, rebuild_schema


MISSING_TEXT = "brak danych"

RAW_COLUMN_MAPPING = {
    "patientunitstayid": "care_unit_stay_id",
    "patienthealthsystemstayid": "admission_id",
    "gender": "gender",
    "age": "age",
    "ethnicity": "ethnicity",
    "hospitalid": "hospital_id",
    "wardid": "ward_id",
    "apacheadmissiondx": "diagnosis",
    "admissionheight": "admission_height",
    "hospitaladmittime24": "hospital_admit_time",
    "hospitaladmitoffset": "hospital_admit_offset",
    "hospitaladmitsource": "hospital_admit_source",
    "hospitaldischargeyear": "discharge_year",
    "hospitaldischargetime24": "hospital_discharge_time",
    "hospitaldischargeoffset": "hospital_discharge_offset",
    "hospitaldischargelocation": "hospital_discharge_location",
    "hospitaldischargestatus": "hospital_discharge_status",
    "unittype": "unit_type",
    "unitadmittime24": "unit_admit_time",
    "unitadmitsource": "unit_admit_source",
    "unitvisitnumber": "unit_visit_number",
    "unitstaytype": "unit_stay_type",
    "admissionweight": "admission_weight",
    "dischargeweight": "discharge_weight",
    "unitdischargetime24": "unit_discharge_time",
    "unitdischargeoffset": "unit_discharge_offset",
    "unitdischargelocation": "unit_discharge_location",
    "unitdischargestatus": "unit_discharge_status",
    "uniquepid": "patient_id",
}

CSV_COLUMNS = list(RAW_COLUMN_MAPPING.keys())
NORMALIZED_COLUMNS = [
    "care_unit_stay_id",
    "admission_id",
    "gender",
    "age",
    "ethnicity",
    "hospital_id",
    "ward_id",
    "diagnosis",
    "admission_height",
    "hospital_admit_time",
    "hospital_admit_offset",
    "hospital_admit_source",
    "discharge_year",
    "hospital_discharge_time",
    "hospital_discharge_offset",
    "hospital_discharge_location",
    "hospital_discharge_status",
    "unit_type",
    "unit_admit_time",
    "unit_admit_source",
    "unit_visit_number",
    "unit_stay_type",
    "admission_weight",
    "discharge_weight",
    "weight_change_kg",
    "unit_discharge_time",
    "unit_discharge_offset",
    "unit_discharge_location",
    "unit_discharge_status",
    "patient_id",
]

INTEGER_COLUMNS = [
    "care_unit_stay_id",
    "admission_id",
    "hospital_id",
    "ward_id",
    "hospital_admit_offset",
    "discharge_year",
    "hospital_discharge_offset",
    "unit_visit_number",
    "unit_discharge_offset",
]

FLOAT_COLUMNS = [
    "admission_height",
    "admission_weight",
    "discharge_weight",
]

TEXT_FILL_COLUMNS = [
    "gender",
    "ethnicity",
    "diagnosis",
    "hospital_admit_time",
    "hospital_admit_source",
    "hospital_discharge_time",
    "hospital_discharge_location",
    "hospital_discharge_status",
    "unit_type",
    "unit_admit_time",
    "unit_admit_source",
    "unit_stay_type",
    "unit_discharge_time",
    "unit_discharge_location",
    "unit_discharge_status",
]

REQUIRED_IDENTIFIER_COLUMNS = {
    "care_unit_stay_id": "identyfikator pobytu oddziałowego",
    "admission_id": "identyfikator hospitalizacji",
    "patient_id": "identyfikator pacjenta",
}

DIAGNOSIS_TRANSLATIONS = {
    "CHF, congestive heart failure": "CHF, zastoinowa niewydolność serca",
    "CVA, cerebrovascular accident/stroke": "CVA, udar mózgu",
    "Infarction, acute myocardial (MI)": "Ostry zawał mięśnia sercowego, MI",
    "Angina, stable (asymp or stable pattern of symptoms w/meds)": (
        "Stabilna dławica piersiowa"
    ),
    "Angina, unstable (angina interferes w/quality of life or meds are tolerated poorly)": (
        "Niestabilna dławica piersiowa"
    ),
    "Diabetic ketoacidosis": "Cukrzycowa kwasica ketonowa",
    "Renal failure, acute": "Ostra niewydolność nerek",
    "Respiratory - medical, other": "Inna choroba układu oddechowego",
    "Emphysema/bronchitis": "Rozedma lub zapalenie oskrzeli",
    "Cardiomyopathy": "Kardiomiopatia",
    "Cardiac arrest (with or without respiratory arrest; for respiratory arrest see Respiratory System)": (
        "Zatrzymanie krążenia"
    ),
    "Coma/change in level of consciousness (for hepatic see GI, for diabetic see Endocrine, if related to cardiac arrest, see CV)": (
        "Śpiączka lub zaburzenia świadomości"
    ),
    "CABG alone, coronary artery bypass grafting": (
        "Izolowane CABG, pomostowanie aortalno-wieńcowe"
    ),
    "CABG with aortic valve replacement": "CABG z wymianą zastawki aortalnej",
    "CABG with other operation": "CABG z innym zabiegiem",
    "CABG, minimally invasive; mid-CABG": "Małoinwazyjne CABG",
    "Aortic valve replacement (isolated)": "Izolowana wymiana zastawki aortalnej",
    "Aortic and Mitral valve replacement": "Wymiana zastawki aortalnej i mitralnej",
    "Mitral valve repair": "Naprawa zastawki mitralnej",
    "Mitral valve replacement": "Wymiana zastawki mitralnej",
    "Tricuspid valve surgery": "Operacja zastawki trójdzielnej",
    "Ventricular Septal Defect (VSD) Repair": (
        "Naprawa ubytku przegrody międzykomorowej, VSD"
    ),
    "ARDS-adult respiratory distress syndrome, non-cardiogenic pulmonary edema": (
        "ARDS, niekardiogenny obrzęk płuc"
    ),
    "Acid-base/electrolyte disturbance": "Zaburzenia kwasowo-zasadowe i elektrolitowe",
    "Cardiovascular medical, other": "Inna choroba układu krążenia",
    "Cardiovascular surgery, other": "Inny zabieg układu krążenia",
    "Neurologic medical, other": "Inna choroba neurologiczna",
    "Neurologic surgery, other": "Inny zabieg neurochirurgiczny",
    "GI medical, other": "Inna choroba przewodu pokarmowego",
    "GI surgery, other": "Inny zabieg przewodu pokarmowego",
    "Genitourinary medical, other": "Inna choroba układu moczowo-płciowego",
    "Genitourinary surgery, other": "Inny zabieg układu moczowo-płciowego",
    "Metabolic/endocrine medical, other": "Inna choroba metaboliczna lub endokrynologiczna",
    "Musculoskeletal medical, other": "Inna choroba układu mięśniowo-szkieletowego",
    "Vascular medical, other": "Inna choroba naczyniowa",
    "Vascular surgery, other": "Inny zabieg chirurgii naczyniowej",
    "Pneumothorax": "Odma opłucnowa",
    "Pancreatitis": "Zapalenie trzustki",
    "Endocarditis": "Zapalenie wsierdzia",
    "Meningitis": "Zapalenie opon mózgowo-rdzeniowych",
    "Hypoglycemia": "Hipoglikemia",
    "Hypothermia": "Hipotermia",
    "Coagulopathy": "Koagulopatia",
    "Atelectasis": "Niedodma",
    "Cholangitis": "Zapalenie dróg żółciowych",
    "Anemia": "Niedokrwistość",
    "Anaphylaxis": "Anafilaksja",
    "Myasthenia gravis": "Miastenia",
    "Abdomen only trauma": "Uraz ograniczony do jamy brzusznej",
    "Ablation or mapping of cardiac conduction pathway": (
        "Ablacja lub mapowanie układu przewodzenia serca"
    ),
    "Abscess, neurologic": "Ropień neurologiczny",
    "Alcohol withdrawal": "Zespół odstawienia alkoholu",
    "Amputation (non-traumatic)": "Amputacja nieurazowa",
    "Aneurysm, dissecting aortic": "Tętniak rozwarstwiający aorty",
    "Aneurysm, thoracic aortic": "Tętniak aorty piersiowej",
    "Aneurysm, thoracic aortic; with dissection": (
        "Tętniak aorty piersiowej z rozwarstwieniem"
    ),
    "Aneurysm/pseudoaneurysm, other": "Inny tętniak lub tętniak rzekomy",
    "Aneurysms, repair of other (except ventricular)": (
        "Naprawa innych tętniaków, z wyłączeniem komorowych"
    ),
    "Arrest, respiratory (without cardiac arrest)": (
        "Zatrzymanie oddechu bez zatrzymania krążenia"
    ),
    "Biopsy, open lung": "Otwarta biopsja płuca",
    "Bleeding-upper GI, surgery for": (
        "Operacja z powodu krwawienia z górnego odcinka przewodu pokarmowego"
    ),
    "Burr hole placement": "Założenie otworu trepanacyjnego",
    "Cancer, pancreatic": "Rak trzustki",
    "Cancer, stomach": "Rak żołądka",
    "Cancer-colon/rectal, surgery for (including abdominoperineal resections)": (
        "Operacja raka okrężnicy lub odbytnicy"
    ),
    "Cancer-esophageal, surgery for (abdominal approach)": (
        "Operacja raka przełyku, dostęp brzuszny"
    ),
    "Cancer-small intestinal, surgery for": "Operacja raka jelita cienkiego",
    "Cellulitis and localized soft tissue infections": (
        "Cellulitis i miejscowe zakażenia tkanek miękkich"
    ),
    "Cellulitis and localized soft tissue infections, surgery for": (
        "Operacja z powodu cellulitis lub miejscowego zakażenia tkanek miękkich"
    ),
    "Cesarean section": "Cięcie cesarskie",
    "Chest pain, atypical (noncardiac chest pain)": (
        "Nietypowy ból w klatce piersiowej, niekardiologiczny"
    ),
    "Chest pain, epigastric": "Ból w nadbrzuszu",
    "Chest pain, unknown origin": "Ból w klatce piersiowej o nieznanym pochodzeniu",
    "Chest thorax only trauma": "Uraz ograniczony do klatki piersiowej",
    "Chest/extremity trauma": "Uraz klatki piersiowej i kończyn",
    "Chest/multiple trauma": "Uraz wielonarządowy z zajęciem klatki piersiowej",
    "Chest/pelvis trauma": "Uraz klatki piersiowej i miednicy",
    "Chest/spinal trauma": "Uraz klatki piersiowej i kręgosłupa",
    "Complications of prev. peripheral vasc. surgery,surgery for (i.e.ligation of bleeder, exploration and evacuation of hematoma, debridement, pseudoaneurysms, clots, fistula, etc.)": (
        "Operacja z powodu powikłań wcześniejszej operacji naczyń obwodowych"
    ),
    "Complications of previous GI surgery; surgery for (anastomotic leak, bleeding, abscess, infection, dehiscence, etc.)": (
        "Operacja z powodu powikłań po wcześniejszej operacji przewodu pokarmowego"
    ),
    "Complications of previous open-heart surgery, surgery for (i.e. bleeding, infection, mediastinal rewiring,leaking aortic graft etc.)": (
        "Operacja z powodu powikłań po wcześniejszej kardiochirurgii na otwartym sercu"
    ),
    "Cranioplasty and complications from previous craniotomies": (
        "Kranioplastyka i powikłania po wcześniejszych kraniotomiach"
    ),
    "Cystectomy for neoplasm": "Cystektomia z powodu nowotworu",
    "Cystectomy, other reasons": "Cystektomia z innych przyczyn",
    "Defibrillator, automatic implantable cardiac; insertion of": (
        "Wszczepienie automatycznego kardiowertera-defibrylatora"
    ),
    "Diabetic hyperglycemic hyperosmolar nonketotic coma (HHNC)": (
        "HHNC, hiperglikemiczny stan hiperosmolarny bez ketozy"
    ),
    "Diverticular disease": "Choroba uchyłkowa",
    "Diverticular disease, surgery for": "Operacja z powodu choroby uchyłkowej",
    "Drug withdrawal": "Zespół odstawienia substancji",
    "Efffusion, pericardial": "Wysięk osierdziowy",
    "Effusions, pleural": "Wysięk opłucnowy",
    "Embolectomy (with general anesthesia)": "Embolektomia w znieczuleniu ogólnym",
    "Embolectomy (without general anesthesia)": "Embolektomia bez znieczulenia ogólnego",
    "Embolus, pulmonary": "Zatorowość płucna",
    "Encephalopathies (excluding hepatic)": "Encefalopatie z wyłączeniem wątrobowej",
    "Encephalopathy, hepatic": "Encefalopatia wątrobowa",
    "Endarterectomy (other vessels)": "Endarterektomia innych naczyń",
    "Endarterectomy, carotid": "Endarterektomia tętnicy szyjnej",
    "Esophageal surgery, other": "Inny zabieg chirurgiczny przełyku",
    "Extremity only trauma": "Uraz ograniczony do kończyn",
    "Extremity only trauma, surgery for": "Operacja z powodu urazu kończyn",
    "Facial surgery (if related to trauma, see Trauma)": "Operacja twarzy",
    "Fistula/abscess, surgery for (not inflammatory bowel disease)": (
        "Operacja z powodu przetoki lub ropnia, poza nieswoistym zapaleniem jelit"
    ),
    "Fracture-pathological, non-union, non-traumatic, for fractures due to trauma see Trauma": (
        "Złamanie patologiczne lub staw rzekomy, nieurazowe"
    ),
    "GI obstruction": "Niedrożność przewodu pokarmowego",
    "GI obstruction, surgery for (including lysis of adhesions)": (
        "Operacja z powodu niedrożności przewodu pokarmowego"
    ),
    "GI perforation/rupture": "Perforacja lub pęknięcie przewodu pokarmowego",
    "GI perforation/rupture, surgery for": (
        "Operacja z powodu perforacji lub pęknięcia przewodu pokarmowego"
    ),
    "GI vascular insufficiency": "Niewydolność naczyniowa przewodu pokarmowego",
    "GI vascular ischemia, surgery for (resection)": (
        "Operacja z powodu niedokrwienia przewodu pokarmowego"
    ),
    "Gastrostomy": "Gastrostomia",
    "Graft for dialysis, insertion of": "Założenie przeszczepu naczyniowego do dializ",
    "Graft, all other bypass (except renal)": (
        "Inny pomost naczyniowy, z wyłączeniem nerkowego"
    ),
    "Graft, femoral-popliteal bypass": "Pomost udowo-podkolanowy",
    "Head only trauma": "Uraz ograniczony do głowy",
    "Head/chest trauma": "Uraz głowy i klatki piersiowej",
    "Head/extremity trauma": "Uraz głowy i kończyn",
    "Head/multiple trauma": "Uraz wielonarządowy z zajęciem głowy",
    "Heart transplant": "Przeszczep serca",
    "Heat exhaustion/stroke": "Wyczerpanie cieplne lub udar cieplny",
    "Hematologic medical, other": "Inna choroba hematologiczna",
    "Hematoma subdural, surgery for": "Operacja krwiaka podtwardówkowego",
    "Hematoma, subdural": "Krwiak podtwardówkowy",
    "Hematomas": "Krwiaki",
    "Hemorrhage (for gastrointestinal bleeding GI-see GI system) (for trauma see Trauma)": (
        "Krwotok, poza przewodem pokarmowym i urazami"
    ),
    "Hemorrhage, intra/retroperitoneal": "Krwotok wewnątrzotrzewnowy lub zaotrzewnowy",
    "Hemorrhage/hematoma, intracranial": "Krwotok lub krwiak wewnątrzczaszkowy",
    "Hemorrhage/hematoma-intracranial, surgery for": (
        "Operacja krwotoku lub krwiaka wewnątrzczaszkowego"
    ),
    "Hemorrhage/hemoptysis, pulmonary": "Krwotok płucny lub krwioplucie",
    "Hepatic failure, acute": "Ostra niewydolność wątroby",
    "Hip replacement, total (non-traumatic)": (
        "Całkowita endoprotezoplastyka biodra, nieurazowa"
    ),
    "Hydrocephalus, obstructive": "Wodogłowie obturacyjne",
    "Hypertension, uncontrolled (for cerebrovascular accident-see Neurological System)": (
        "Niekontrolowane nadciśnienie tętnicze"
    ),
    "Hypovolemia (including dehydration, Do not include shock states)": (
        "Hipowolemia, w tym odwodnienie"
    ),
    "Hysterectomy for other benign neoplasm/fibroids": (
        "Histerektomia z powodu łagodnego nowotworu lub mięśniaków"
    ),
    "Infection/abscess, other surgery for": "Operacja z powodu innego zakażenia lub ropnia",
    "Inflammatory bowel disease": "Nieswoiste zapalenie jelit",
    "Kidney transplant": "Przeszczep nerki",
    "Laminectomy/spinal cord decompression (excluding malignancies)": (
        "Laminektomia lub odbarczenie rdzenia kręgowego, bez nowotworów"
    ),
    "MI admitted > 24 hrs after onset of ischemia": (
        "MI przyjęty ponad 24 godziny od początku niedokrwienia"
    ),
    "Mastectomy (all)": "Mastektomia",
    "Monitoring, hemodynamic (pre-operative evaluation)": (
        "Monitorowanie hemodynamiczne przedoperacyjne"
    ),
    "Neoplasm, neurologic": "Nowotwór neurologiczny",
    "Neoplasm-cranial, surgery for (excluding transphenoidal)": (
        "Operacja nowotworu wewnątrzczaszkowego, bez dostępu przezklinowego"
    ),
    "Neoplasm-spinal cord, surgery or other related procedures": (
        "Operacja nowotworu rdzenia kręgowego lub zabieg pokrewny"
    ),
    "Nephrectomy for neoplasm": "Nefrektomia z powodu nowotworu",
    "Obesity-morbid, surgery for": "Operacja z powodu otyłości olbrzymiej",
    "Obstruction-airway (i.e., acute epiglottitis, post-extubation edema, foreign body, etc)": (
        "Niedrożność dróg oddechowych"
    ),
    "Obstruction/other, surgery for (with or without ileal conduit)": (
        "Operacja z powodu innej niedrożności"
    ),
    "Orthopedic surgery, other": "Inny zabieg ortopedyczny",
    "Pelvis/hip trauma": "Uraz miednicy lub biodra",
    "Renal bleeding": "Krwawienie nerkowe",
    "Renal infection/abscess": "Zakażenie lub ropień nerki",
    "Renal obstruction": "Niedrożność dróg moczowych",
    "Respiratory surgery, other": "Inny zabieg chirurgiczny układu oddechowego",
    "Restrictive lung disease (i.e., Sarcoidosis, pulmonary fibrosis)": (
        "Restrykcyjna choroba płuc"
    ),
    "Seizures (primary-no structural brain disease)": (
        "Napady drgawkowe pierwotne, bez strukturalnej choroby mózgu"
    ),
    "Shock, cardiogenic": "Wstrząs kardiogenny",
    "Shunts and revisions": "Zespolenia i ich rewizje",
    "Spinal cord surgery, other": "Inny zabieg chirurgiczny rdzenia kręgowego",
    "Spinal/face trauma": "Uraz kręgosłupa i twarzy",
    "Subarachnoid hemorrhage/intracranial aneurysm": (
        "Krwotok podpajęczynówkowy lub tętniak wewnątrzczaszkowy"
    ),
    "Tamponade, pericardial": "Tamponada osierdzia",
    "Thoracotomy for bronchopleural fistula": (
        "Torakotomia z powodu przetoki oskrzelowo-opłucnowej"
    ),
    "Thoracotomy for lung cancer": "Torakotomia z powodu raka płuca",
    "Thoracotomy for lung reduction": (
        "Torakotomia z powodu zabiegu redukcji objętości płuca"
    ),
    "Thoracotomy for other malignancy in chest": (
        "Torakotomia z powodu innego nowotworu klatki piersiowej"
    ),
    "Thoracotomy for thoracic/respiratory infection": (
        "Torakotomia z powodu zakażenia klatki piersiowej lub układu oddechowego"
    ),
    "Thrombectomy (with general anesthesia)": "Trombektomia w znieczuleniu ogólnym",
    "Thrombosis, vascular (deep vein)": "Zakrzepica naczyniowa, żył głębokich",
    "Thrombus, arterial": "Zakrzep tętniczy",
    "Transphenoidal surgery": "Operacja przezklinowa",
    "Tumor removal, intracardiac": "Usunięcie guza wewnątrzsercowego",
    "Weaning from mechanical ventilation (transfer from other unit or hospital only)": (
        "Odłączanie od wentylacji mechanicznej po transferze"
    ),
    "Whipple-surgery for pancreatic cancer": "Operacja Whipple'a z powodu raka trzustki",
}

SEPSIS_TRANSLATIONS = {
    "GI": "Sepsa pochodzenia z przewodu pokarmowego",
    "cutaneous/soft tissue": "Sepsa pochodzenia skórnego lub z tkanek miękkich",
    "other": "Sepsa o innym pochodzeniu",
    "pulmonary": "Sepsa pochodzenia płucnego",
    "renal/UTI (including bladder)": "Sepsa pochodzenia nerkowego lub z ZUM",
    "unknown": "Sepsa o nieustalonym pochodzeniu",
}

PNEUMONIA_TRANSLATIONS = {
    "aspiration": "Zachłystowe zapalenie płuc",
    "bacterial": "Bakteryjne zapalenie płuc",
    "other": "Inne zapalenie płuc",
    "viral": "Wirusowe zapalenie płuc",
}

BLEEDING_TRANSLATIONS = {
    "GI-location unknown": "Krwawienie z przewodu pokarmowego, lokalizacja nieznana",
    "lower GI": "Krwawienie z dolnego odcinka przewodu pokarmowego",
    "upper GI": "Krwawienie z górnego odcinka przewodu pokarmowego",
}

RHYTHM_TRANSLATIONS = {
    "atrial, supraventricular": "Zaburzenia rytmu nadkomorowe lub przedsionkowe",
    "conduction defect": "Zaburzenia rytmu z zaburzeniami przewodzenia",
    "ventricular": "Komorowe zaburzenia rytmu",
}

OVERDOSE_TRANSLATIONS = {
    "alcohols (bethanol, methanol, ethylene glycol)": "Przedawkowanie alkoholi toksycznych",
    "analgesic (aspirin, acetaminophen)": "Przedawkowanie leków przeciwbólowych",
    "antidepressants (cyclic, lithium)": "Przedawkowanie leków przeciwdepresyjnych",
    "other toxin, poison or drug": "Przedawkowanie innej toksyny, trucizny lub leku",
    "sedatives, hypnotics, antipsychotics, benzodiazepines": (
        "Przedawkowanie leków sedacyjnych, nasennych, przeciwpsychotycznych lub benzodiazepin"
    ),
    "street drugs (opiates, cocaine, amphetamine)": "Przedawkowanie narkotyków ulicznych",
}


@dataclass(frozen=True)
class ImportStats:
    raw_rows: int
    patients: int
    hospital_stays: int
    unit_stays: int
    patient_ethnicity_conflicts: int
    patient_age_conflicts: int
    hospital_diagnosis_conflicts: int


def import_csv_to_database(csv_path: str | Path, db_path: str | Path) -> ImportStats:
    """Import one CSV file, rebuild the schema and return a compact import summary."""
    resolved_csv = Path(csv_path)
    if not resolved_csv.exists():
        raise FileNotFoundError(f"CSV file not found: {resolved_csv}")

    df = pd.read_csv(resolved_csv)
    df.columns = [column.strip() for column in df.columns]

    missing_columns = [column for column in CSV_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(
            "CSV schema mismatch. Missing columns: " + ", ".join(missing_columns)
        )

    cleaned_df = _prepare_clean_dataframe(df)
    raw_df = _prepare_raw_import(cleaned_df)
    patients_df = _prepare_patients(cleaned_df)
    admissions_df = _prepare_admissions(cleaned_df)
    care_unit_stays_df = _prepare_care_unit_stays(cleaned_df)

    _write_import_to_database(
        db_path,
        raw_df,
        patients_df,
        admissions_df,
        care_unit_stays_df,
    )

    return ImportStats(
        raw_rows=len(raw_df),
        patients=len(patients_df),
        hospital_stays=len(admissions_df),
        unit_stays=len(care_unit_stays_df),
        patient_ethnicity_conflicts=_count_conflicts(
            cleaned_df,
            "patient_id",
            "ethnicity",
        ),
        patient_age_conflicts=_count_conflicts(cleaned_df, "patient_id", "age"),
        hospital_diagnosis_conflicts=_count_conflicts(
            cleaned_df,
            "admission_id",
            "diagnosis",
        ),
    )


def _prepare_clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all domain cleaning rules before any data is written to SQLite."""
    cleaned_df = df[CSV_COLUMNS].copy()
    cleaned_df = cleaned_df.replace(r"^\s*$", pd.NA, regex=True)
    cleaned_df = cleaned_df.rename(columns=RAW_COLUMN_MAPPING)

    for column in TEXT_FILL_COLUMNS + ["patient_id"]:
        cleaned_df[column] = cleaned_df[column].map(_clean_text)

    for column in INTEGER_COLUMNS:
        cleaned_df[column] = _to_nullable_int(cleaned_df[column])

    for column in FLOAT_COLUMNS:
        cleaned_df[column] = _to_nullable_float(cleaned_df[column])

    # Keep only one final age column in the analytical dataset.
    cleaned_df["age"] = cleaned_df["age"].map(_clean_age).astype("Int64")
    cleaned_df["unit_type"] = cleaned_df["unit_type"].map(_clean_unit_type)
    cleaned_df["diagnosis"] = cleaned_df["diagnosis"].map(_translate_diagnosis)
    # Missing discharge weight inherits admission weight by project requirement.
    cleaned_df["discharge_weight"] = cleaned_df["discharge_weight"].fillna(
        cleaned_df["admission_weight"]
    )
    cleaned_df["weight_change_kg"] = _calculate_weight_change(
        cleaned_df["admission_weight"],
        cleaned_df["discharge_weight"],
    )

    cleaned_df = _fill_missing_text(cleaned_df)
    _validate_required_identifiers(cleaned_df)
    return cleaned_df[NORMALIZED_COLUMNS]


def _write_import_to_database(
    db_path: str | Path,
    raw_df: pd.DataFrame,
    patients_df: pd.DataFrame,
    admissions_df: pd.DataFrame,
    care_unit_stays_df: pd.DataFrame,
) -> None:
    db_target = str(db_path)
    if db_target != ":memory:" and not db_target.startswith("file:"):
        resolved_db = Path(db_target)
        resolved_db.parent.mkdir(parents=True, exist_ok=True)
        target = resolved_db
    else:
        target = db_target

    with get_connection(target) as connection:
        # A CSV import is treated as a full refresh of the working dataset.
        rebuild_schema(connection)
        _write_import_batch(
            connection,
            raw_df,
            patients_df,
            admissions_df,
            care_unit_stays_df,
        )


def _write_frames(
    connection,
    raw_df: pd.DataFrame,
    patients_df: pd.DataFrame,
    admissions_df: pd.DataFrame,
    care_unit_stays_df: pd.DataFrame,
) -> None:
    raw_df.to_sql("import_rows", connection, if_exists="append", index=False)
    patients_df.to_sql("patients", connection, if_exists="append", index=False)
    admissions_df.to_sql("admissions", connection, if_exists="append", index=False)
    care_unit_stays_df.to_sql(
        "care_unit_stays",
        connection,
        if_exists="append",
        index=False,
    )


def _write_import_batch(
    connection,
    raw_df: pd.DataFrame,
    patients_df: pd.DataFrame,
    admissions_df: pd.DataFrame,
    care_unit_stays_df: pd.DataFrame,
) -> None:
    """Write a single import as one transaction."""
    try:
        connection.execute("BEGIN IMMEDIATE;")
        _write_frames(connection, raw_df, patients_df, admissions_df, care_unit_stays_df)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _prepare_raw_import(df: pd.DataFrame) -> pd.DataFrame:
    raw_df = df.copy()
    raw_df.insert(0, "import_row_id", range(1, len(raw_df) + 1))
    return _replace_na_with_none(raw_df)


def _prepare_patients(df: pd.DataFrame) -> pd.DataFrame:
    patients_df = (
        df.groupby("patient_id", dropna=False)
        .agg(
            gender=("gender", _canonical_text),
            ethnicity=("ethnicity", _canonical_text),
        )
        .reset_index()
    )
    return _replace_na_with_none(patients_df)


def _prepare_admissions(df: pd.DataFrame) -> pd.DataFrame:
    admissions_df = (
        df.groupby("admission_id", dropna=False)
        .agg(
            patient_id=("patient_id", _canonical_text),
            age=("age", _canonical_number),
            hospital_id=("hospital_id", _canonical_number),
            admission_height=("admission_height", _canonical_number),
            hospital_admit_time=("hospital_admit_time", _canonical_text),
            hospital_admit_source=("hospital_admit_source", _canonical_text),
            discharge_year=("discharge_year", _canonical_number),
            hospital_discharge_time=("hospital_discharge_time", _canonical_text),
            hospital_discharge_location=(
                "hospital_discharge_location",
                _canonical_text,
            ),
            hospital_discharge_status=("hospital_discharge_status", _canonical_text),
            related_rows=("care_unit_stay_id", "count"),
        )
        .reset_index()
    )

    for column in ["admission_id", "age", "hospital_id", "discharge_year", "related_rows"]:
        admissions_df[column] = pd.to_numeric(
            admissions_df[column],
            errors="coerce",
        ).astype("Int64")

    admissions_df["admission_height"] = pd.to_numeric(
        admissions_df["admission_height"],
        errors="coerce",
    )

    return _replace_na_with_none(admissions_df)


def _prepare_care_unit_stays(df: pd.DataFrame) -> pd.DataFrame:
    care_unit_stays_df = df[
        [
            "care_unit_stay_id",
            "admission_id",
            "ward_id",
            "diagnosis",
            "hospital_admit_offset",
            "hospital_discharge_offset",
            "unit_type",
            "unit_admit_time",
            "unit_admit_source",
            "unit_visit_number",
            "unit_stay_type",
            "admission_weight",
            "discharge_weight",
            "weight_change_kg",
            "unit_discharge_time",
            "unit_discharge_offset",
            "unit_discharge_location",
            "unit_discharge_status",
        ]
    ].copy()

    for column in INTEGER_COLUMNS:
        if column in care_unit_stays_df.columns:
            care_unit_stays_df[column] = pd.to_numeric(
                care_unit_stays_df[column],
                errors="coerce",
            ).astype("Int64")

    for column in ["admission_weight", "discharge_weight", "weight_change_kg"]:
        care_unit_stays_df[column] = pd.to_numeric(
            care_unit_stays_df[column],
            errors="coerce",
        )

    return _replace_na_with_none(care_unit_stays_df)

def _replace_na_with_none(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notna(df), None)


def _fill_missing_text(df: pd.DataFrame) -> pd.DataFrame:
    """Replace empty text fields with a user-facing placeholder."""
    filled_df = df.copy()
    for column in TEXT_FILL_COLUMNS:
        filled_df[column] = filled_df[column].fillna(MISSING_TEXT)
    return filled_df


def _validate_required_identifiers(df: pd.DataFrame) -> None:
    invalid_columns: list[str] = []
    for column, label in REQUIRED_IDENTIFIER_COLUMNS.items():
        if df[column].isna().any():
            invalid_columns.append(label)

    if invalid_columns:
        raise ValueError(
            "CSV zawiera puste wartości w wymaganych kolumnach: "
            + ", ".join(invalid_columns)
        )


def _clean_text(value) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    return re.sub(r"\s+", " ", text)


def _normalize_number_text(value) -> str | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None

    return cleaned.replace(",", ".")


def _to_nullable_int(series: pd.Series) -> pd.Series:
    normalized = series.map(_normalize_number_text)
    return pd.to_numeric(normalized, errors="coerce").astype("Int64")


def _to_nullable_float(series: pd.Series) -> pd.Series:
    normalized = series.map(_normalize_number_text)
    return pd.to_numeric(normalized, errors="coerce")


def _clean_age(value) -> int | pd.NA:
    """Convert '>89' to 89 and keep only numeric ages."""
    cleaned = _clean_text(value)
    if cleaned is None:
        return pd.NA

    compact = cleaned.replace(" ", "")
    if compact == ">89":
        return 89

    numeric_value = pd.to_numeric(_normalize_number_text(cleaned), errors="coerce")
    if pd.isna(numeric_value):
        return pd.NA

    return int(float(numeric_value))


def _clean_unit_type(value) -> str | None:
    """Remove abbreviations placed in parentheses from care unit names."""
    cleaned = _clean_text(value)
    if cleaned is None:
        return None

    without_parentheses = re.sub(r"\s*\([^)]*\)", "", cleaned)
    normalized = re.sub(r"\s+", " ", without_parentheses).strip(" -/")
    return normalized or None


def _translate_diagnosis(value) -> str | None:
    """Translate diagnosis labels into controlled Polish terminology."""
    cleaned = _clean_text(value)
    if cleaned is None:
        return None

    translated = DIAGNOSIS_TRANSLATIONS.get(cleaned)
    if translated is not None:
        return translated

    if cleaned.startswith("Sepsis, "):
        return SEPSIS_TRANSLATIONS.get(cleaned.removeprefix("Sepsis, "), cleaned)

    if cleaned.startswith("Pneumonia, "):
        return PNEUMONIA_TRANSLATIONS.get(cleaned.removeprefix("Pneumonia, "), cleaned)

    if cleaned.startswith("Bleeding, "):
        return BLEEDING_TRANSLATIONS.get(cleaned.removeprefix("Bleeding, "), cleaned)

    if cleaned.startswith("Rhythm disturbance (") and cleaned.endswith(")"):
        inner_text = cleaned.removeprefix("Rhythm disturbance (").removesuffix(")")
        return RHYTHM_TRANSLATIONS.get(inner_text, cleaned)

    if cleaned.startswith("Overdose, "):
        return OVERDOSE_TRANSLATIONS.get(cleaned.removeprefix("Overdose, "), cleaned)

    return cleaned


def _calculate_weight_change(
    admission_weight: pd.Series,
    discharge_weight: pd.Series,
) -> pd.Series:
    return (discharge_weight - admission_weight).round(2)


def _canonical_text(series: pd.Series) -> str | None:
    cleaned = [value for value in (_clean_text(item) for item in series) if value]
    if not cleaned:
        return None

    non_missing = [value for value in cleaned if value != MISSING_TEXT]
    values_to_count = non_missing if non_missing else cleaned
    counts = pd.Series(values_to_count).value_counts()
    top_frequency = counts.iloc[0]
    candidates = sorted(
        value for value, count in counts.items() if count == top_frequency
    )
    return candidates[0]


def _canonical_number(series: pd.Series) -> int | float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None

    modes = numeric.mode(dropna=True)
    value = modes.iloc[0] if not modes.empty else numeric.iloc[0]
    return int(value) if float(value).is_integer() else float(value)


def _count_conflicts(
    df: pd.DataFrame,
    group_column: str,
    value_column: str,
) -> int:
    non_null = df[[group_column, value_column]].dropna(subset=[value_column]).copy()
    if non_null.empty:
        return 0

    if (
        pd.api.types.is_string_dtype(non_null[value_column])
        or non_null[value_column].dtype == object
    ):
        non_null = non_null[non_null[value_column] != MISSING_TEXT]
        if non_null.empty:
            return 0

    counts = non_null.groupby(group_column)[value_column].nunique(dropna=True)
    return int((counts > 1).sum())
