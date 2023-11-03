USE [ubpd_base]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_ICMP]    Script Date: 3/11/2023 9:57:53 a. m. ******/
DROP PROCEDURE [dbo].[CONSULTA_V_ICMP]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_ICMP]    Script Date: 3/11/2023 9:57:53 a. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





-- =============================================
-- Author:		Juan Daniel Molano Castro
-- Create date: 10/10/2023
-- Description:	Realiza la consulta de las personas desaparecidas de la fuente V_ICMP
-- =============================================
CREATE   PROCEDURE [dbo].[CONSULTA_V_ICMP]
WITH RECOMPILE 
AS
BEGIN
  SET NOCOUNT ON;
  SELECT 
	personas.fuente,
	personas.[Nombre Completo] AS nombre_completo,
	personas.[Primer Nombre] AS primer_nombre,
	personas.[Segundo Nombre] AS segundo_nombre,
	personas.[Primer Apellido] AS primer_apellido,
    personas.[Segundo Apellido] AS segundo_apellido, 
	personas.documento, 
	personas.sexo, 
	personas.[Pais de ocurrencia] AS pais_ocurrencia,
	personas.edad_des_inf,
    personas.edad_des_sup,
    personas.anio_nacimiento, 
	personas.anio_nacimiento_ini,
    personas.anio_nacimiento_fin,
	personas.mes_nacimiento, 
	personas.dia_nacimiento,
    personas.iden_pertenenciaetnica,
	personas.codigo_dane_departamento,
    personas.codigo_dane_municipio,
	personas.fecha_ocur_dia,
    personas.fecha_ocur_mes,
	personas.fecha_ocur_anio,
    personas.[Presunto responsable] AS presunto_responsable,
	personas.[Codigo unico fuente] AS codigo_unico_fuente,
	personas.[Tipo de hecho] AS tipo_de_hecho,
	personas.[Descripcion_Relato] AS descripcion_relato,
	personas.[Situacion_actual_des]situacion_actual_des
	FROM orq_salida.ICMP AS personas

  END
  
GO


