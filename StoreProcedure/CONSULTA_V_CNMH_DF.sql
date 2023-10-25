USE [ubpd_base]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_CNMH_DF]    Script Date: 25/10/2023 10:14:02 a. m. ******/
DROP PROCEDURE [dbo].[CONSULTA_V_CNMH_DF]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_CNMH_DF]    Script Date: 25/10/2023 10:14:02 a. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



-- =============================================
-- Author:		Juan Daniel Molano Castro
-- Create date: 10/10/2023
-- Description:	Realiza la consulta de las personas desaparecidas de la fuente CNMH_RU
-- =============================================
CREATE   PROCEDURE [dbo].[CONSULTA_V_CNMH_DF]
WITH RECOMPILE 
AS
BEGIN
  SET NOCOUNT ON;
  SELECT 
	convert(varchar,personas.IdCaso) + '_' + personas.IdentificadorCaso + '_' + personas.Id  AS codigo_unico_fuente,
	convert(varchar,personas.IdCaso) AS IdCaso,
	personas.IdentificadorCaso,
	personas.Id,
	personas.estado,
	personas.ZonIdLugarDelHecho AS zon_id_lugar_del_hecho,
	personas.MUNICIPIO_CASO AS municipio_caso, 
	personas.DEPTO_CASO AS depto_caso,
	personas.ANNOH,
	personas.MESH,
	personas.DIAH,
    personas.Nacionalidad AS nacionalidad, 
	personas.tipo_documento, 
	personas.NumeroDocumento AS numero_documento,
	personas.NombresApellidos AS nombres_apellidos,
    personas.SobreNombreAlias AS sobre_nombre_alias, 
	personas.sexo,
	personas.FechaNacimiento AS fecha_nacimiento,
	personas.Orientación_Sexual AS orientacion_sexual,
    personas.DescripcionEdad AS descripcion_edad, 
	personas.etnia, 
	personas.DescripcionEtnia AS descripcion_etnia, 
	personas.discapacidad,
    personas.OcupacionVictima AS ocupacion_victima,
	personas.DescripcionOtraOcupacionVictima AS descripcion_otra_ocupacion_victima,
    personas.CalidadVictima AS calidad_victima,
	personas.CargoRangoFuncionarioPublico AS cargo_rango_funcionario_publico,
    personas.CargoEmpleadoSectorPrivado AS cargo_empleado_sector_privado,
	personas.TipoPoblacionVulnerable AS tipo_poblacion_vulnerable,
    personas.DescripcionOtroTipoPoblacionVulnerable AS descripcion_otro_tipo_poblacion_vulnerable,
	personas.Organización_Civil AS organizacion_civil,
    personas.MilitantePolitico AS militante_politico,
	personas.DescripcionOtroMilitantePolitico AS descripcion_otro_militante_politico,
    personas.grupo,
	personas.DESCRIPCION_GRUPO AS descripcion_grupo, 
	personas.EspeficicacionPresuntoResponsable AS espeficicacion_presunto_responsable,
    personas.ObservacionesGrupoArmado1 AS observaciones_grupo_armado1,
	personas.RangoFuerzasArmadas AS rango_fuerzas_armadas,
    personas.Descripción_Rango_Fuerzas_Armadas_Estatales AS descripcion_rango_fuerzas_armadas_estatales,
	personas.RangoGrupoArmado AS rango_grupo_armado,
    personas.DescripcionRangoGrupoArmado AS descripcion_rango_grupo_armado,
	personas.AccionesBusquedaFamilias AS acciones_busqueda_familias,
    personas.ACTV_MEC_BUS AS actv_mec_bus, 
	personas.SituacionActualVictima AS situacion_actual_victima,
    personas.FuenteInformacionDesaparicion AS fuente_informacion_desaparicion,
	personas.RAD_SEN_JUD AS rad_sen_jud, 
	personas.infjev, 
	personas.confesion,
    personas.VIOL_CUERPO_NOMBRE AS viol_cuerpo_nombre,
	personas.SIG_VIOL_CUERPO AS sig_viol_cuerpo,
	personas.SignosViolenciaSexual AS signos_violencia_sexual,
    personas.desc_sig_vs,
	personas.DisposicionCuerpo AS disposicion_cuerpo,
	personas.d_disp_cuerpo, 
	personas.depto_aparic,
    personas.vereda_sitio_ap,
	personas.esc_aparic, 
	personas.reg_hechos_gao, 
	personas.entidad_recep_denun,
    personas.mun_denun, 
	personas.depto_denun,
	casos.PerpetradorIdentificado AS perpetrador_identificado,
	casos.PRESUNTO_REPONSABLE AS presunto_reponsable,
	casos.DESCRIPCION_PRESUNTO_RESPONSABLE1 AS descripcion_presunto_responsable1,
	casos.EspeficicacionPresuntoResponsable AS espeficicacion_presunto_responsable_2,
	casos.Observaciones_Presunto_Responsable1 AS observaciones_presunto_responsable1,
	casos.TipoDesaparicion AS tipo_desaparicion,
	casos.OtroHechoSimultaneo AS otro_hecho_simultaneo,
	casos.DescripcionDelCaso AS descripcion_del_caso
  FROM [dbo].[V_CNMH_DF] personas 
    LEFT JOIN [dbo].[V_CNMH_DF_C] casos 
	ON casos.IdCaso = personas.IdCaso

  END
  
GO

