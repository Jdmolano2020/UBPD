USE [ubpd_base]
GO
/****** Object:  StoredProcedure [dbo].[VALIDA_V_FGN_INACTIVOS_UNIVERSO]    Script Date: 23/11/2023 12:02:56 p. m. ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





-- =============================================
-- Author:		Juan Daniel Molano Castro
-- Create date: 10/10/2023
-- Description:	Realiza la consulta de las personas desaparecidas de la fuente V_ICMP
-- =============================================
ALTER   PROCEDURE [dbo].[VALIDA_V_FGN_INACTIVOS_UNIVERSO]
WITH RECOMPILE 
AS
BEGIN
  SET NOCOUNT ON;
  IF OBJECT_ID('[dbo].[BD_FGN_INACTIVOS_VAL]', 'U') IS NOT NULL
    DROP TABLE [dbo].[BD_FGN_INACTIVOS_VAL]

  SELECT A.[tabla_origen]
	  ,C.[codigo_unico_fuente] AS codigo_unico_fuente_o	
      ,A.[codigo_unico_fuente] AS codigo_unico_fuente_u
      ,B.[codigo_unico_fuente] AS codigo_unico_fuente_i
	  ,CASE WHEN Coalesce(A.[codigo_unico_fuente],'Nulo') = Coalesce(B.[codigo_unico_fuente],'Nulo') THEN 'Igual' ELSE 'Diferente' END AS codigo_unico_fuente_v
      ,A.[nombre_completo] AS nombre_completo_u
      ,B.[nombre_completo] AS nombre_completo_i
	  ,CASE WHEN Coalesce(A.[nombre_completo],'Nulo') = Coalesce(B.[nombre_completo],'Nulo') THEN 'Igual' ELSE 'Diferente' END AS nombre_completo_v
      ,A.[primer_nombre] AS primer_nombre_u
      ,B.[primer_nombre] AS primer_nombre_i
	  ,CASE WHEN Coalesce(A.[primer_nombre],'Nulo') = Coalesce(B.[primer_nombre],'Nulo') THEN 'Igual' ELSE 'Diferente' END AS primer_nombre_v
      ,A.[segundo_nombre] AS segundo_nombre_u
      ,B.[segundo_nombre] AS segundo_nombre_i
	  ,CASE WHEN Coalesce(A.[segundo_nombre],'Nulo') = Coalesce(B.[segundo_nombre],'Nulo') THEN 'Igual' ELSE 'Diferente' END AS segundo_nombre_v
      ,A.[primer_apellido] AS primer_apellido_u
      ,B.[primer_apellido] AS primer_apellido_i
	  ,CASE WHEN Coalesce(A.[primer_apellido],'Nulo') = Coalesce(B.[primer_apellido],'Nulo') THEN 'Igual' ELSE 'Diferente' END AS primer_apellido_v
      ,A.[segundo_apellido] AS segundo_apellido_u
      ,B.[segundo_apellido] AS segundo_apellido_i
	  ,CASE WHEN Coalesce(A.[segundo_apellido],'Nulo') = Coalesce(B.[segundo_apellido],'Nulo') THEN 'Igual' ELSE 'Diferente' END AS segundo_apellido_v
      ,A.[documento] AS documento_u
      ,B.[documento] AS documento_i
	  ,CASE WHEN Coalesce(A.[documento],'Nulo') = Coalesce(B.[documento],'Nulo') THEN 'Igual' ELSE 'Diferente' END AS documento_v
      ,A.[sexo] AS sexo_u
      ,B.[sexo] AS sexo_i
	  ,CASE WHEN Coalesce(A.[sexo],'Nulo') = Coalesce(B.[sexo],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS sexo_v
	  ,A.[iden_pertenenciaetnica] AS iden_pertenenciaetnic_u
	  ,B.[iden_pertenenciaetnica] AS iden_pertenenciaetnic_i
	  ,CASE WHEN Coalesce(A.[iden_pertenenciaetnica],'Nulo') = Coalesce(B.[iden_pertenenciaetnica],'Nulo') THEN 'Igual' ELSE 'Diferente' END AS iden_pertenenciaetnic_v
	  --,A.[fecha_nacimiento] AS fecha_nacimiento_u
	  --,B. [fecha_nacimiento] AS fecha_nacimiento_i
	  --,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[fecha_nacimiento]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[fecha_nacimiento]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS fecha_nacimiento_v
   --   ,A.[anio_nacimiento] AS anio_nacimiento_u
   --   ,B.[anio_nacimiento] AS anio_nacimiento_i
	  --,CASE WHEN Coalesce(A.[anio_nacimiento],'Nulo') = Coalesce(B.[anio_nacimiento],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS anio_nacimiento_v
   --   ,A.[mes_nacimiento] AS mes_nacimiento_u
   --   ,B.[mes_nacimiento] AS mes_nacimiento_i
	  --,CASE WHEN Coalesce(A.[mes_nacimiento],'Nulo') = Coalesce(B.[mes_nacimiento],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS mes_nacimiento_v
   --   ,A.[dia_nacimiento] AS dia_nacimiento_u
   --   ,B.[dia_nacimiento] AS dia_nacimiento_i
	  --,CASE WHEN Coalesce(A.[dia_nacimiento],'Nulo') = Coalesce(B.[dia_nacimiento],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS dia_nacimiento_v
      ,A.[edad] AS edad_u
      ,B.[edad] AS edad_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[edad]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[edad]),'Nulo') THEN 'Igual' ELSE 'Diferente' END AS edad_v
      ,A.[fecha_desaparicion] AS [fecha_desaparicion_u]
      ,B.[fecha_desaparicion] AS fecha_desaparicion_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[fecha_desaparicion]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[fecha_desaparicion]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS fecha_desaparicion_v
	  ,A.[fecha_ocur_anio] AS fecha_ocur_anio_u
	  ,B.[fecha_ocur_anio] AS fecha_ocur_anio_i
	  ,CASE WHEN Coalesce(A.[fecha_ocur_anio],'Nulo') = Coalesce(B.[fecha_ocur_anio],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS fecha_ocur_anio_v
	  ,A.[fecha_ocur_mes] AS fecha_ocur_mes_u
	  ,B.[fecha_ocur_mes] AS fecha_ocur_mes_i
	  ,CASE WHEN Coalesce(A.[fecha_ocur_mes],'Nulo') = Coalesce(B.[fecha_ocur_mes],'Nulo') THEN 'Igual' ELSE 'Diferente' END   AS fecha_ocur_mes_v
      ,A.[fecha_ocur_dia] AS fecha_ocur_dia_u
      ,B.[fecha_ocur_dia] AS fecha_ocur_dia_i
	  ,CASE WHEN Coalesce(A.[fecha_ocur_dia],'Nulo') = Coalesce(B.[fecha_ocur_dia],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS fecha_ocur_dia_v
      ,A.[codigo_dane_departamento] AS codigo_dane_departamento_u
      ,B.[codigo_dane_departamento] AS codigo_dane_departamento_i
	  ,CASE WHEN Coalesce(A.[codigo_dane_departamento],'Nulo') = Coalesce(B.[codigo_dane_departamento],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS codigo_dane_departamento_v
      ,A.[departamento_ocurrencia] AS departamento_ocurrencia_u
      ,B.[departamento_ocurrencia] AS departamento_ocurrencia_i
	  ,CASE WHEN Coalesce(A.[departamento_ocurrencia],'Nulo') = Coalesce(B.[departamento_ocurrencia],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS departamento_ocurrencia_v
      ,A.[codigo_dane_municipio] AS codigo_dane_municipi_u
      ,B.[codigo_dane_municipio] AS codigo_dane_municipi_i
	  ,CASE WHEN Coalesce(A.[codigo_dane_municipio],'Nulo') = Coalesce(B.[codigo_dane_municipio],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS codigo_dane_municipi_v
      ,A.[municipio_ocurrencia] AS municipio_ocurrencia_u
      ,B.[municipio_ocurrencia] AS municipio_ocurrencia_i
	  ,CASE WHEN Coalesce(A.[municipio_ocurrencia],'Nulo') = Coalesce(B.[municipio_ocurrencia],'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS municipio_ocurrencia_v
      ,A.[TH_DF] AS TH_DF_u
      ,B.[TH_DF] AS TH_DF_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[TH_DF]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[TH_DF]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS TH_DF_v
      ,A.[TH_SE] AS TH_SE_u
      ,B.[TH_SE] AS TH_SE_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[TH_SE]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[TH_SE]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS TH_SE_v
      ,A.[TH_RU] AS TH_RU_u
      ,B.[TH_RU] AS TH_RU_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[TH_RU]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[TH_RU]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS TH_RU_v
	  ,A.[TH_OTRO] AS TH_OTRO_u
      ,B.[TH_OTRO] AS TH_OTRO_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[TH_OTRO]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[TH_OTRO]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS TH_OTRO_v
      ,A.[pres_resp_paramilitares] AS pres_resp_paramilitares_u
      ,B.[pres_resp_paramilitares] AS pres_resp_paramilitares_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[pres_resp_paramilitares]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[pres_resp_paramilitares]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS pres_resp_paramilitares_v
      ,A.[pres_resp_grupos_posdesmov] AS pres_resp_grupos_posdesmov_u
      ,B.[pres_resp_grupos_posdesmov] AS pres_resp_grupos_posdesmov_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[pres_resp_grupos_posdesmov]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[pres_resp_grupos_posdesmov]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS pres_resp_grupos_posdesmov_v
      ,A.[pres_resp_agentes_estatales] AS pres_resp_agentes_estatales_u
      ,B.[pres_resp_agentes_estatales] AS pres_resp_agentes_estatales_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[pres_resp_agentes_estatales]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[pres_resp_agentes_estatales]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS pres_resp_agentes_estatales_v
      ,A.[pres_resp_guerr_farc] AS pres_resp_guerr_farc_u
      ,B.[pres_resp_guerr_farc] AS pres_resp_guerr_farc_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[pres_resp_guerr_farc]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[pres_resp_guerr_farc]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS pres_resp_guerr_farc_v
      ,A.[pres_resp_guerr_eln] AS pres_resp_guerr_eln_u
      ,B.[pres_resp_guerr_eln] AS pres_resp_guerr_eln_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[pres_resp_guerr_eln]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[pres_resp_guerr_eln]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS pres_resp_guerr_eln_v
      ,A.[pres_resp_guerr_otra]	AS pres_resp_guerr_otra_u
      ,B.[pres_resp_guerr_otra]	AS pres_resp_guerr_otra_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[pres_resp_guerr_otra]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[pres_resp_guerr_otra]),'Nulo') THEN 'Igual' ELSE 'Diferente' END 	AS pres_resp_guerr_otra_v
      ,A.[pres_resp_otro] AS pres_resp_otro_u
      ,B.[pres_resp_otro] AS pres_resp_otro_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[pres_resp_otro]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[pres_resp_otro]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS pres_resp_otro_v
   --   ,A.[situacion_actual_des] AS situacion_actual_des_u
   --   ,B.[situacion_actual_des] AS situacion_actual_des_i
	  --,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[situacion_actual_des]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[situacion_actual_des]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS situacion_actual_des_v
      ,A.[descripcion_relato] AS descripcion_relato_u
      ,B.[descripcion_relato] AS descripcion_relato_i
	  ,CASE WHEN Coalesce(CONVERT(VARCHAR,A.[descripcion_relato]),'Nulo') = Coalesce(CONVERT(VARCHAR,B.[descripcion_relato]),'Nulo') THEN 'Igual' ELSE 'Diferente' END  AS descripcion_relato_v
	  ,CONVERT(VARCHAR(MAX),'Iguales') AS campos_revisar
	  INTO [dbo].[BD_FGN_INACTIVOS_VAL]
  FROM [orq_salida].[FGN_INACTIVOS] C LEFT JOIN [dbo].[FGN_INACTIVOS_U] A
			ON C.[codigo_unico_fuente] = A.[codigo_unico_fuente]
			    AND A.tabla_origen = 'FGN_EXP_INACTIVOS'
		LEFT JOIN [dbo].[BD_FGN_INACTIVOS] B 
			ON C.[codigo_unico_fuente] =  B.[codigo_unico_fuente]

	UPDATE A SET campos_revisar =	CASE WHEN [codigo_unico_fuente_v] = 'Diferente' THEN 'codigo_unico_fuente;' ELSE '' END +
								CASE WHEN [nombre_completo_v] = 'Diferente' THEN 'nombre_completo;' ELSE '' END	+					
								CASE WHEN [primer_nombre_v] = 'Diferente' THEN 'primer_nombre;' ELSE '' END +
								CASE WHEN [segundo_nombre_v] = 'Diferente' THEN 'segundo_nombre;' ELSE '' END +
								CASE WHEN [primer_apellido_v] = 'Diferente' THEN 'primer_apellido;' ELSE '' END +
								CASE WHEN [segundo_apellido_v] = 'Diferente' THEN 'segundo_apellido;' ELSE '' END +
								CASE WHEN [documento_v] = 'Diferente' THEN 'documento;' ELSE '' END +
								CASE WHEN [sexo_v] = 'Diferente' THEN 'sexo;' ELSE '' END +
								CASE WHEN [iden_pertenenciaetnic_v] = 'Diferente' THEN 'iden_pertenenciaetnica;' ELSE '' END +
								--CASE WHEN [fecha_nacimiento_v] = 'Diferente' THEN 'fecha_nacimiento;' ELSE '' END +
								--CASE WHEN [anio_nacimiento_v] = 'Diferente' THEN 'anio_nacimiento;' ELSE '' END +
								--CASE WHEN [mes_nacimiento_v] = 'Diferente' THEN 'mes_nacimiento;' ELSE '' END +
								--CASE WHEN [dia_nacimiento_v] = 'Diferente' THEN 'dia_nacimiento;' ELSE '' END +
								CASE WHEN [edad_v] = 'Diferente' THEN 'edad;' ELSE '' END +
								CASE WHEN [fecha_desaparicion_v] = 'Diferente' THEN 'fecha_desaparicion;' ELSE '' END +
								CASE WHEN [fecha_ocur_anio_v] = 'Diferente' THEN 'fecha_ocur_anio;' ELSE '' END +
								CASE WHEN [fecha_ocur_mes_v] = 'Diferente' THEN 'fecha_ocur_mes;' ELSE '' END +
								CASE WHEN [fecha_ocur_dia_v] = 'Diferente' THEN 'fecha_ocur_dia;' ELSE '' END +
								CASE WHEN [codigo_dane_departamento_v] = 'Diferente' THEN 'codigo_dane_departamento;' ELSE '' END +
								CASE WHEN [departamento_ocurrencia_v] = 'Diferente' THEN 'departamento_ocurrencia;' ELSE '' END +
								CASE WHEN [codigo_dane_municipi_v] = 'Diferente' THEN 'codigo_dane_municipio;' ELSE '' END +
								CASE WHEN [municipio_ocurrencia_v] = 'Diferente' THEN 'municipio_ocurrencia;' ELSE '' END +
								CASE WHEN [TH_DF_v] = 'Diferente' THEN 'TH_DF;' ELSE '' END +
								CASE WHEN [TH_SE_v] = 'Diferente' THEN 'TH_SE;' ELSE '' END +
								CASE WHEN [TH_RU_v] = 'Diferente' THEN 'TH_RU;' ELSE '' END +
								CASE WHEN [TH_OTRO_v] = 'Diferente' THEN 'TH_OTRO;' ELSE '' END +
								CASE WHEN [pres_resp_paramilitares_v] = 'Diferente' THEN 'pres_resp_paramilitares;' ELSE '' END +
								CASE WHEN [pres_resp_grupos_posdesmov_v] = 'Diferente' THEN 'pres_resp_grupos_posdesmov;' ELSE '' END +
								CASE WHEN [pres_resp_agentes_estatales_v] = 'Diferente' THEN 'pres_resp_agentes_estatales;' ELSE '' END +
								CASE WHEN [pres_resp_guerr_farc_v] = 'Diferente' THEN 'pres_resp_guerr_farc;' ELSE '' END +
								CASE WHEN [pres_resp_guerr_eln_v] = 'Diferente' THEN 'pres_resp_guerr_eln;' ELSE '' END +
								CASE WHEN [pres_resp_guerr_otra_v] = 'Diferente' THEN 'pres_resp_guerr_otra;' ELSE '' END +
								CASE WHEN [pres_resp_otro_v] = 'Diferente' THEN 'pres_resp_otro;' ELSE '' END +
								--CASE WHEN [situacion_actual_des_v] = 'Diferente' THEN 'situacion_actual_des;' ELSE '' END +
								CASE WHEN [descripcion_relato_v] = 'Diferente' THEN 'descripcion_relato;' ELSE '' END
	FROM [dbo].[BD_FGN_INACTIVOS_VAL] A

	SELECT [tabla_origen]
	  ,[codigo_unico_fuente_o]	
      ,[codigo_unico_fuente_u]
      ,[codigo_unico_fuente_i]
      ,[codigo_unico_fuente_v]
      ,[nombre_completo_u]
      ,[nombre_completo_i]
      ,[nombre_completo_v]
      ,[primer_nombre_u]
      ,[primer_nombre_i]
      ,[primer_nombre_v]
      ,[segundo_nombre_u]
      ,[segundo_nombre_i]
      ,[segundo_nombre_v]
      ,[primer_apellido_u]
      ,[primer_apellido_i]
      ,[primer_apellido_v]
      ,[segundo_apellido_u]
      ,[segundo_apellido_i]
      ,[segundo_apellido_v]
      ,[documento_u]
      ,[documento_i]
      ,[documento_v]
      ,[sexo_u]
      ,[sexo_i]
      ,[sexo_v]
      ,[iden_pertenenciaetnic_u]
      ,[iden_pertenenciaetnic_i]
      ,[iden_pertenenciaetnic_v]
      --,[fecha_nacimiento_u]
      --,[fecha_nacimiento_i]
      --,[fecha_nacimiento_v]
      --,[anio_nacimiento_u]
      --,[anio_nacimiento_i]
      --,[anio_nacimiento_v]
      --,[mes_nacimiento_u]
      --,[mes_nacimiento_i]
      --,[mes_nacimiento_v]
      --,[dia_nacimiento_u]
      --,[dia_nacimiento_i]
      --,[dia_nacimiento_v]
      ,[edad_u]
      ,[edad_i]
      ,[edad_v]
      ,[fecha_desaparicion_u]
      ,[fecha_desaparicion_i]
      ,[fecha_desaparicion_v]
      ,[fecha_ocur_anio_u]
      ,[fecha_ocur_anio_i]
      ,[fecha_ocur_anio_v]
      ,[fecha_ocur_mes_u]
      ,[fecha_ocur_mes_i]
      ,[fecha_ocur_mes_v]
      ,[fecha_ocur_dia_u]
      ,[fecha_ocur_dia_i]
      ,[fecha_ocur_dia_v]
      ,[codigo_dane_departamento_u]
      ,[codigo_dane_departamento_i]
      ,[codigo_dane_departamento_v]
      ,[departamento_ocurrencia_u]
      ,[departamento_ocurrencia_i]
      ,[departamento_ocurrencia_v]
      ,[codigo_dane_municipi_u]
      ,[codigo_dane_municipi_i]
      ,[codigo_dane_municipi_v]
      ,[municipio_ocurrencia_u]
      ,[municipio_ocurrencia_i]
      ,[municipio_ocurrencia_v]
      ,[TH_DF_u]
      ,[TH_DF_i]
      ,[TH_DF_v]
      ,[TH_SE_u]
      ,[TH_SE_i]
      ,[TH_SE_v]
      ,[TH_RU_u]
      ,[TH_RU_i]
      ,[TH_RU_v]
	  ,[TH_OTRO_u]
      ,[TH_OTRO_i]
      ,[TH_OTRO_v]
      ,[pres_resp_paramilitares_u]
      ,[pres_resp_paramilitares_i]
      ,[pres_resp_paramilitares_v]
      ,[pres_resp_grupos_posdesmov_u]
      ,[pres_resp_grupos_posdesmov_i]
      ,[pres_resp_grupos_posdesmov_v]
      ,[pres_resp_agentes_estatales_u]
      ,[pres_resp_agentes_estatales_i]
      ,[pres_resp_agentes_estatales_v]
      ,[pres_resp_guerr_farc_u]
      ,[pres_resp_guerr_farc_i]
      ,[pres_resp_guerr_farc_v]
      ,[pres_resp_guerr_eln_u]
      ,[pres_resp_guerr_eln_i]
      ,[pres_resp_guerr_eln_v]
      ,[pres_resp_guerr_otra_u]
      ,[pres_resp_guerr_otra_i]
      ,[pres_resp_guerr_otra_v]
      ,[pres_resp_otro_u]
      ,[pres_resp_otro_i]
      ,[pres_resp_otro_v]
      --,[situacion_actual_des_u]
      --,[situacion_actual_des_i]
      --,[situacion_actual_des_v]
      ,[descripcion_relato_u]
      ,[descripcion_relato_i]
      ,[descripcion_relato_v]
	  ,campos_revisar
  FROM [dbo].[BD_FGN_INACTIVOS_VAL]
  END
  
