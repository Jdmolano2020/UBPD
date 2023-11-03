USE [ubpd_base]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_CNMH_SE]    Script Date: 3/11/2023 9:57:03 a. m. ******/
DROP PROCEDURE [dbo].[CONSULTA_V_CNMH_SE]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_CNMH_SE]    Script Date: 3/11/2023 9:57:03 a. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



-- =============================================
-- Author:		Juan Daniel Molano Castro
-- Create date: 10/10/2023
-- Description:	Realiza la consulta de las personas desaparecidas de la fuente CNMH_RU
-- =============================================
CREATE   PROCEDURE [dbo].[CONSULTA_V_CNMH_SE]
WITH RECOMPILE 
AS
BEGIN
  SET NOCOUNT ON;
  SELECT 
	personas.IdCaso + '_' + personas.IdentificadorCaso + '_' + Id  AS codigo_unico_fuente,
	personas.estado,
	personas.ZonIdLugarDelHecho AS zon_id_lugar_del_hecho,
	personas.MUNINICIO_CASO AS municipio_caso, 
	personas.depto_caso,
    personas.nacionalidad, 
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
	personas.SituacionActualVictima AS situacion_actual_victima,
	personas.Observaciones_Situación_Actual_de_la_Víctima AS observaciones_situacion_actual_de_la_victima,
	personas.circustancia_muerte_en_cautiverio,
	personas.Descripción_Otra_Circustancia_Muerte_en_Cautiverio AS descripcion_otra_circustancia_muerte_en_cautiverio,
	personas.Tipo_Liberación AS tipo_liberacion,
	personas.Descripción_Otro_Tipo_Liberación AS descripcion_otro_tipo_liberacion,
	personas.Días_Cautiverio AS dias_cautiverio,
	personas.no_veces_secuestrado,
	personas.Hechos_Simultáneos_Durante_Periodo AS hechos_simultaneos_durante_periodo,
	personas.Otro_Hecho_Simultáneos_Durante_Periodo AS otro_hecho_simultaneos_durante_periodo,
	personas.grupo,
	personas.descripcion_grupo,
	personas.EspeficicacionPresuntoResponsable AS espeficicacion_presunto_responsable,
	personas.ObservacionesGrupoArmado1 AS observaciones_grupo_armado1,
	personas.RangoFuerzasArmadas AS rango_fuerzas_armadas,
	personas.Descripción_Rango_Fuerzas_Armadas_Estatales AS descripcion_rango_fuerzas_armadas_estatales,
	personas.RangoGrupoArmado AS rango_grupo_armado,
	personas.DescripcionRangoGrupoArmado AS descripcion_rango_grupo_armado,
	personas.MESH AS mesh,
	personas.DIAH AS diah,
	personas.ANNOH AS annoh,
	casos.ZonIdLugarDelHecho AS zon_id_lugar_del_hecho_2,
	casos.MUNINICIO_CASO AS municipio_caso_2,
	casos.DEPTO_CASO AS depto_caso_2,
	casos.region,
	casos.cabecera_municipal,
	casos.comuna,
	casos.barrio,
	casos.area_rural,
	casos.corregimiento,
	casos.vereda,
	casos.CódigoCentroPoblado AS codigo_centro_poblado,
	casos.CentroPoblado AS centro_poblado,
	casos.TipoCentroPoblado AS tipo_centro_poblado,
	casos.sitio,
	casos.territorio_colectivo,
	casos.resguardo,
	casos.modalidad,
	casos.Descripción_de_la_Modalidad AS descripcion_de_la_modalidad,
	casos.modalidad_de_secuestro,
	casos.tipo_secuestro,
	casos.finalidad_del_secuestro,
	casos.descripcion_otra_finalidad,
	casos.Exigencia_para_la_Liberación AS exigencia_para_la_liberacion,
	casos.DescripcionOtraExigencia AS descripcion_otra_exigencia,
	casos.PorteListas AS porte_listas,
	casos.IngresoViviendaFinca AS ingreso_vivienda_finca,
	casos.encapuchados,
	casos.PerpetradorIdentificado AS perpetrador_identificado,
	casos.presunto_reponsable,
	casos.descripcion_presunto_responsable1,
	casos.EspeficicacionPresuntoResponsable AS espeficicacion_presunto_responsable_2,
	casos.observaciones_presunto_responsable,
	casos.AbandonoDespojoForzadoTierras AS abandono_despojo_forzado_tierras,
	casos.AmenazaIntimidacion AS amenaza_intimidacion,
	casos.AtaqueContraMisionMedica AS ataque_contra_mision_medica,
	casos.ConfinamientoRestriccionMovilidad AS confinamiento_restriccion_movilidad,
	casos.DesplazamientoForzado AS desplazamiento_forzado,
	casos.extorsion,
	casos.LesionadosCiviles AS lesionados_civiles,
	casos.pillaje,
	casos.tortura,
	casos.ViolenciaBasadaGenero AS violencia_basada_genero,
	casos.OtroHechoSimultaneo AS otro_hecho_simultaneo,
	casos.total_civiles,
	casos.total_combatientes,
	casos.total_civiles_combatientes,
	casos.GrafitisLetreros AS grafitis_letreros,
	casos.VinculosFamiliares AS vinculos_familiares,
	casos.MujeresEmbarazadas AS mujeres_embarazadas,
	casos.DescripcionDelCaso AS descripcion_del_caso,
	casos.usuario,
	casos.Estado AS estado_2,
	casos.TipoCaso AS tipo_caso,
	casos.CasoMaestro AS caso_maestro
  FROM [dbo].[V_CNMH_SE] personas 
	LEFT JOIN [dbo].[V_CNMH_SE_C] casos 
	On casos.IdCaso = personas.IdCaso

  END
GO


