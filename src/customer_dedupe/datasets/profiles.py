from __future__ import annotations

from customer_dedupe.schema import FieldTag, RecordSchema

# Flattened version of your provided source columns.
RETAIL_COLUMNS = [
    "CUSTOMER_PK",
    "DIM_CUSTOMER_ID",
    "REGISTERED_DATE",
    "ANONYMISED",
    "DIM_INDIVIDUAL_ID",
    "WEB_CUSTOMER_ID",
    "TITLE",
    "FIRSTNAME",
    "LASTNAME",
    "GENDER",
    "SOURCE",
    "DOB",
    "EMAIL",
    "COUNTRY_CODE",
    "LAST_UPDATED",
    "DELIVERY_ADDRESS_COUNT",
    "BILLING_TITLE",
    "BILLING_FIRSTNAME",
    "BILLING_LASTNAME",
    "BILLING_ADDRESS_LINE1",
    "BILLING_ADDRESS_LINE2",
    "BILLING_ADDRESS_LINE3",
    "BILLING_TOWN",
    "BILLING_POSTCODE",
    "BILLING_COUNTRY_CODE",
    "BILLING_PHONE",
    "CONTACT_WOMEN",
    "CONTACT_MEN",
    "CONTACT_KIDS",
    "CONTACT_BEAUTY",
    "OPTED_IN_TO_MARKETING",
    "AGGREGATED_MARKETING_PREFERENCE",
    "GDPR_ANONYMISED",
    "GDPR_REGISTERED_DATE",
]


RETAIL_SCHEMA = RecordSchema.from_mapping(
    {
        FieldTag.CUSTOMER_ID: ["CUSTOMER_PK", "DIM_CUSTOMER_ID", "WEB_CUSTOMER_ID"],
        FieldTag.NAME: ["TITLE", "FIRSTNAME", "LASTNAME", "BILLING_FIRSTNAME", "BILLING_LASTNAME"],
        FieldTag.EMAIL: ["EMAIL"],
        FieldTag.DOB: ["DOB"],
        FieldTag.GENDER: ["GENDER"],
        FieldTag.COUNTRY: ["COUNTRY_CODE", "BILLING_COUNTRY_CODE"],
        FieldTag.ADDRESS: [
            "BILLING_ADDRESS_LINE1",
            "BILLING_ADDRESS_LINE2",
            "BILLING_ADDRESS_LINE3",
            "BILLING_TOWN",
        ],
        FieldTag.POSTCODE: ["BILLING_POSTCODE"],
        FieldTag.PHONE: ["BILLING_PHONE"],
        FieldTag.DATE: ["REGISTERED_DATE", "LAST_UPDATED", "GDPR_REGISTERED_DATE"],
        FieldTag.MARKETING: [
            "CONTACT_WOMEN",
            "CONTACT_MEN",
            "CONTACT_KIDS",
            "CONTACT_BEAUTY",
            "OPTED_IN_TO_MARKETING",
            "AGGREGATED_MARKETING_PREFERENCE",
        ],
    }
)
