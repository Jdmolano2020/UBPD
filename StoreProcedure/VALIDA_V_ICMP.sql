USE [ubpd_base]
GO

/****** Object:  StoredProcedure [dbo].[VALIDA_V_ICMP]    Script Date: 3/11/2023 9:59:49 a. m. ******/
DROP PROCEDURE [dbo].[VALIDA_V_ICMP]
GO

/****** Object:  StoredProcedure [dbo].[VALIDA_V_ICMP]    Script Date: 3/11/2023 9:59:49 a. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






-- =============================================
-- Author:		Juan Daniel Molano Castro
-- Create date: 10/10/2023
-- Description:	Realiza la consulta de las personas desaparecidas de la fuente V_ICMP
-- =============================================
CREATE   PROCEDURE [dbo].[VALIDA_V_ICMP]
WITH RECOMPILE 
AS
BEGIN
  SET NOCOUNT ON;
  SELECT [tabla_origen]
      ,[codigo_unico_fuente]
      ,[nombre_completo]
      ,[primer_nombre]
      ,[segundo_nombre]
      ,[primer_apellido]
      ,[segundo_apellido]
      ,[documento]
      ,[sexo]
	  ,[iden_pertenenciaetnica]
	  ,[fecha_nacimiento]
      ,[anio_nacimiento]
      ,[mes_nacimiento]
      ,[dia_nacimiento]
      ,[edad]
      ,[fecha_desaparicion] AS [fecha_desaparicion_dtf]
	  ,[fecha_ocur_anio]
	  ,[fecha_ocur_mes]
      ,[fecha_ocur_dia]
      ,[codigo_dane_departamento]
      ,[departamento_ocurrencia]
      ,[codigo_dane_municipio]
      ,[municipio_ocurrencia]
      ,[TH_DF]
      ,[TH_SE]
      ,[TH_RU]
      ,[pres_resp_paramilitares]
      ,[pres_resp_grupos_posdesmov]
      ,[pres_resp_agentes_estatales]
      ,[pres_resp_guerr_farc]
      ,[pres_resp_guerr_eln]
      ,[pres_resp_guerr_otra]
      ,[pres_resp_otro]
      ,[situacion_actual_des]
      ,[descripcion_relato]
  FROM [dbo].[BD_UNIVERSO]
  WHERE tabla_origen = 'ICMP'

  END
  
GO


