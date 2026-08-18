CREATE TABLE public.project_new (
    id character varying(100) NOT NULL ENCODE lzo COLLATE case_sensitive,
    name character varying(100) ENCODE lzo COLLATE case_sensitive,
    age character varying(100) ENCODE lzo COLLATE case_sensitive,
    gender character varying(100) ENCODE lzo COLLATE case_sensitive,
    department character varying(100) ENCODE lzo COLLATE case_sensitive,
    position character varying(100) ENCODE lzo COLLATE case_sensitive,
    salary character varying(100) ENCODE lzo COLLATE case_sensitive,
    joining_date date ENCODE az64,
    experience_years character varying(100) ENCODE lzo COLLATE case_sensitive,
    last_modified_timestamp timestamp without time zone ENCODE az64,
    historical_flag character varying(100) ENCODE lzo COLLATE case_sensitive,
    joining_date_string character varying(65535) ENCODE lzo COLLATE case_sensitive,
    last_modified_timestamp_string character varying(65535) ENCODE lzo COLLATE case_sensitive,
    incremental_flag character varying(100) ENCODE lzo COLLATE case_sensitive
) DISTSTYLE AUTO;