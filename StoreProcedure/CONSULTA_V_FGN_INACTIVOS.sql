USE [ubpd_base]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_FGN_INACTIVOS]    Script Date: 3/11/2023 9:57:28 a. m. ******/
DROP PROCEDURE [dbo].[CONSULTA_V_FGN_INACTIVOS]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_FGN_INACTIVOS]    Script Date: 3/11/2023 9:57:28 a. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





-- =============================================
-- Author:		Juan Daniel Molano Castro
-- Create date: 10/10/2023
-- Description:	Realiza la consulta de las personas desaparecidas de la fuente V_FGN_INACTIVOS
-- =============================================
CREATE   PROCEDURE [dbo].[CONSULTA_V_FGN_INACTIVOS]
WITH RECOMPILE 
AS
BEGIN
  SET NOCOUNT ON;
  SELECT 
	personas.primer_nombre,
	personas.segundo_nombre,
	personas.primer_apellido, 
	personas.segundo_apellido, 
	personas.nombre_completo,
	personas.documento,
	personas.edad_des_inf, 
	personas.edad_des_sup, 
	personas.dia_nacimiento, 
	personas.mes_nacimiento,
	personas.anio_nacimiento_ini,
	personas.anio_nacimiento_fin,
	personas.sexo, 
	personas.codigo_dane_departamento,
	personas.codigo_dane_municipio, 
	personas.fecha_ocur_dia, 
	personas.fecha_ocur_mes, 
	personas.fecha_ocur_anio,
	personas.tipo_de_hecho,
	personas.presunto_responsable,
	personas.codigo_unico_fuente,
	personas.iden_orientacionsexual,
	personas.[iden_pertenenciaetnica ] AS iden_pertenenciaetnica_,
	personas.descripcion_relato,
	personas.situacion_actual_des,
	personas.pais_de_ocurrencia
  FROM orq_salida.FGN_INACTIVOS AS personas
  WHERE LEN([nombre_completo])>0
  END
GO


