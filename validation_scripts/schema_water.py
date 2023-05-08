from pandera import DataFrameSchema, Column, Check, Index, MultiIndex

schema = DataFrameSchema(
    columns={
        "access": Column(str,required = True, coerce= True, description = "Access to the water tap"),
        "address": Column(str, required = True, coerce= True, description = "Address of the water tap"),
        "city": Column(str,required = False, coerce= True,nullable = True, description = "City of the water tap"),
        "description": Column(str,required = False, coerce= True,nullable = True, description = "Description of the water tap"),
        "filtration": Column(str,required = False, coerce= True,nullable = True, description = "Filtration of the water tap"),
        "gp_id": Column(str,required = False, coerce= True,nullable = True, description = "ID of the water tap"),
        "handicap": Column(str,required = False, coerce= True,nullable = True, description = "Handicap accessibility of the water tap"),
        "hours": Column(str,required = False, coerce= True,nullable = True, description = "Hours of the water tap"),
        "lat": Column(float,required = False, coerce= True,nullable = True,report_duplicates = 'all',  description = "Latitude of the water tap"),
        "lon": Column(float,required = False, coerce= True,nullable = True, report_duplicates='all', description = "Longitude of the water tap"),
        "norms_rules": Column(str,required = False, coerce= True,nullable = True, description = "Norms and rules of the water tap"),
        "organization": Column(str,required = False, coerce= True,nullable = True, description = "Organization of the water tap"),
        "permanently_closed": Column(bool,required = False, coerce= True,nullable = True, description = "Permanently closed status of the water tap"),
        "phone": Column(str, required = False,coerce= True,nullable = True, description = "Phone number of the water tap"),
        "quality": Column(str,required = False, coerce= True,nullable = True, description = "Quality of the water tap"),
        "service": Column(str,required = False, coerce= True,nullable = True, description = "Service of the water tap"),
        "statement": Column(str,required = False, coerce= True,nullable = True, description = "Statement of the water tap"),
        "status": Column(str,required = False, coerce= True,nullable = True, description = "Status of the water tap"),
        "tap_type": Column(str,required = False, coerce= True,nullable = True, description = "Type of the water tap"),
        "tapnum": Column(float, required = False, coerce= True,nullable = True, report_duplicates='all', description = "Number of the water tap"),
        "vessel": Column(str,required = False, coerce= True,nullable = True, description = "Vessel of the water tap"),
        "zip_code": Column(str,required = False, coerce= True,nullable = True, description = "Zip code of the water tap"),
    },
    strict = True
)
