-- Active: 1691955172011@@192.168.0.4@5432@dados_estacao
select * from gerenciador_tabelas_horarias;

TRUNCATE TABLE gerenciador_tabelas_horarias;

select * from tabelas_horarias."28-10-2022" ORDER BY data_hora;

DROP SCHEMA "tabelas_horarias";

select "tabelas_horarias"."10-08-2023".codigo,
"tabelas_horarias"."10-08-2023".data_hora,
"tabelas_horarias"."10-08-2023".umidade,
"tabelas_horarias"."10-08-2023".pressao,
"tabelas_horarias"."10-08-2023".temp_int,
"tabelas_horarias"."10-08-2023".temp_ext
from gerenciador_tabelas_horarias
inner join tabelas_horarias."10-08-2023"
on gerenciador_tabelas_horarias.codigo=tabelas_horarias."10-08-2023".codigo_gerenciador
where (extract(minute from "tabelas_horarias"."10-08-2023".data_hora)=15 or
extract(minute from "tabelas_horarias"."10-08-2023".data_hora)=30 or
extract(minute from tabelas_horarias."10-08-2023".data_hora)=45 or
extract(minute from tabelas_horarias."10-08-2023".data_hora)=0) and
extract(second from tabelas_horarias."10-08-2023".data_hora)=0
union
select "tabelas_horarias"."11-08-2023".codigo,
"tabelas_horarias"."11-08-2023".data_hora,
"tabelas_horarias"."11-08-2023".umidade,
"tabelas_horarias"."11-08-2023".pressao,
"tabelas_horarias"."11-08-2023".temp_int,
"tabelas_horarias"."11-08-2023".temp_ext
from gerenciador_tabelas_horarias
inner join tabelas_horarias."11-08-2023"
on gerenciador_tabelas_horarias.codigo=tabelas_horarias."11-08-2023".codigo_gerenciador
where (extract(minute from "tabelas_horarias"."11-08-2023".data_hora)=15 or
extract(minute from "tabelas_horarias"."11-08-2023".data_hora)=30 or
extract(minute from tabelas_horarias."11-08-2023".data_hora)=45 or
extract(minute from tabelas_horarias."11-08-2023".data_hora)=0) and
extract(second from tabelas_horarias."11-08-2023".data_hora)=0

group by tabelas_horarias."11-08-2023".codigo 
ORDER BY 
    "tabelas_horarias"."11-08-2023".data_hora;


SELECT
    codigo,
    data_hora,
    umidade,
    pressao,
    temp_int,
    temp_ext
FROM (
    SELECT
        "tabelas_horarias"."10-08-2023".codigo,
        "tabelas_horarias"."10-08-2023".data_hora,
        "tabelas_horarias"."10-08-2023".umidade,
        "tabelas_horarias"."10-08-2023".pressao,
        "tabelas_horarias"."10-08-2023".temp_int,
        "tabelas_horarias"."10-08-2023".temp_ext 
    FROM
        gerenciador_tabelas_horarias 
    INNER JOIN
        tabelas_horarias."10-08-2023" 
    ON
        gerenciador_tabelas_horarias.codigo = tabelas_horarias."10-08-2023".codigo_gerenciador
    WHERE
        (EXTRACT(MINUTE FROM "tabelas_horarias"."10-08-2023".data_hora) = 15 OR 
        EXTRACT(MINUTE FROM "tabelas_horarias"."10-08-2023".data_hora) = 30 OR 
        EXTRACT(MINUTE FROM tabelas_horarias."10-08-2023".data_hora) = 45 OR 
        EXTRACT(MINUTE FROM tabelas_horarias."10-08-2023".data_hora) = 0) AND 
        EXTRACT(SECOND FROM tabelas_horarias."10-08-2023".data_hora) = 0 
    UNION
    SELECT
        "tabelas_horarias"."11-08-2023".codigo,
        "tabelas_horarias"."11-08-2023".data_hora,
        "tabelas_horarias"."11-08-2023".umidade,
        "tabelas_horarias"."11-08-2023".pressao,
        "tabelas_horarias"."11-08-2023".temp_int,
        "tabelas_horarias"."11-08-2023".temp_ext 
    FROM
        gerenciador_tabelas_horarias 
    INNER JOIN
        tabelas_horarias."11-08-2023" 
    ON
        gerenciador_tabelas_horarias.codigo = tabelas_horarias."11-08-2023".codigo_gerenciador
    WHERE
        (EXTRACT(MINUTE FROM "tabelas_horarias"."11-08-2023".data_hora) = 15 OR 
        EXTRACT(MINUTE FROM "tabelas_horarias"."11-08-2023".data_hora) = 30 OR 
        EXTRACT(MINUTE FROM tabelas_horarias."11-08-2023".data_hora) = 45 OR 
        EXTRACT(MINUTE FROM tabelas_horarias."11-08-2023".data_hora) = 0) AND 
        EXTRACT(SECOND FROM tabelas_horarias."11-08-2023".data_hora) = 0
) AS combined_data
ORDER BY
    combined_data.data_hora;





