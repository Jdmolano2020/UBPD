USE [ubpd_base]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_CNMH_RU]    Script Date: 3/11/2023 9:55:40 a. m. ******/
DROP PROCEDURE [dbo].[CONSULTA_V_CNMH_RU]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_CNMH_RU]    Script Date: 3/11/2023 9:55:40 a. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Juan Daniel Molano Castro
-- Create date: 10/10/2023
-- Description:	Realiza la consulta de las personas desaparecidas de la fuente CNMH_RU
-- =============================================
CREATE PROCEDURE [dbo].[CONSULTA_V_CNMH_RU]
WITH RECOMPILE 
AS
BEGIN
  SET NOCOUNT ON;
  SELECT  
	personas.IdCaso + '_' + personas.IdentificadorCaso + '_' + Id  AS codigo_unico_fuente,
	personas.IdCaso, 
	personas.IdentificadorCaso, 
	personas.Estado, 
	personas.ZonIdLugarDelHecho as zon_id_lugar_del_hecho,
	personas.MUNINICIO_CASO AS muninicio_caso, 
	personas.DEPTO_CASO AS depto_caso, 
	personas.ANNOH AS Annoh, 
	personas.MESH AS Mesh, 
	personas.DIAH AS Diah, 
	personas.Id,
	personas.NumeroVictima, 
	personas.FechaIngresoRegistro, 
	personas.Nacionalidad AS nacionalidad,
	personas.Tipo_Documento,
	personas.NumeroDocumento, 
	personas.NombresApellidos AS Nombres_Apellidos,
	personas.SobreNombreAlias AS sobre_nombre_alias, 
	personas.Sexo, 
	personas.Orientación_Sexual AS orientacion_sexual,
	personas.FechaNacimiento, 
	personas.Edad, 
	personas.DescripcionEdad AS descripcion_edad, 
	personas.Etnia,
	personas.DescripcionEtnia AS descripcion_etnia, 
	personas.Discapacidad, 
	personas.OcupacionVictima AS ocupacion_victima,
	personas.DescripcionOtraOcupacionVictima AS descripcion_otra_ocupacion_victima, 
	personas.CalidadVictima AS calidad_victima,
	personas.TipoPoblacionVulnerable AS tipo_poblacion_vulnerable,
	personas.DescripcionOtroTipoPoblacionVulnerable AS descripcion_otro_tipo_poblacion_vulnerable, 
	personas.MilitantePolitico AS militante_politico,
	personas.DescripcionOtroMilitantePolitico AS descripcion_otro_militante_politico, 
	personas.CALETERO AS Caletero, 
	personas.CAMP AS Camp, 
	personas.COCI AS Coci,
	personas.COMAN AS Coman, 
	personas.COMBA AS Comba, 
	personas.CONTA AS Conta, 
	personas.ENTRE AS Entre, 
	personas.ESCO AS Esco, 
	personas.OGAOEXTOR  AS Ogaoextor, 
	personas.FAB_ARM  AS Fab_arm,
	personas.MINAS AS Minas, 
	personas.GUAR AS Guar, 
	personas.INFORM AS Inform, 
	personas.PATRU AS Patru, 
	personas.RADIO AS Radio, 
	personas.RASP AS Rasp, 
	personas.SER_SALUD AS Ser_salud,
	personas.SICA AS Sica, 
	personas.TRA_ORG AS Tra_org, 
	personas.TRA_DRO AS Tra_dro, 
	personas.TRA_ARMAS AS Tra_armas, 
	personas.SIN_OFI AS Sin_ofi, 
	personas.OTRO_OFI AS Otro_ofi,
	personas.HechosSimultaneosDuranteCautiverio AS hechos_simultaneos_durante_cautiverio, 
	personas.DES_H_SIM_PER AS Des_h_sim_per,
	personas.TipoSalida AS tipo_salida, 
	personas.DES_SAL AS Des_sal, 
	personas.MotivoSalida AS motivo_salida, 
	personas.DIAFH  AS Diafh, 
	personas.MESFH AS Mesfh,
	personas.ANNOFH AS Annofh, 
	personas.Grupo, 
	personas.DESCRIPCION_GRUPO AS Descripcion_grupo,
	personas.EspeficicacionPresuntoResponsable AS espeficicacion_presunto_responsable, 
	personas.ObservacionesGrupoArmado1 AS observaciones_grupo_armado1,
	personas.RangoFuerzasArmadas AS rango_fuerzas_armadas,
	personas.Descripción_Rango_Fuerzas_Armadas_Estatales AS descripcion_rango_fuerzas_armadas_estatales, 
	personas.RangoGrupoArmado AS rango_grupo_armado,
	personas.DescripcionRangoGrupoArmado AS descripcion_rango_grupo_armado, 
	personas.MUN_FINALI AS Mun_finali, 
	personas.DEPTO_FINALI AS Depto_finali,
	personas.GrupoSalida AS grupo_salida, 
	personas.DESCRIPCION_GRUPO_SALIDA AS Descripcion_grupo_salida,
	personas.EspeficicacionPresuntoResponsable1 AS espeficicacion_presunto_responsable1, 
	personas.ObservacionesGrupoArmado11 AS observaciones_grupo_armado11,
	personas.NUM_VECRE  AS Num_vecre,
	casos.ZonIdLugarDelHecho AS zon_id_lugar_del_hecho_2,
	casos.MUNINICIO_CASO AS muninicio_caso_2,
	casos.DEPTO_CASO AS depto_caso_2,
	casos.REGION AS region,
	casos.Cabecera_Municipal AS cabecera_municipal,
	casos.Comuna AS comuna,
	casos.BARRIO AS barrio,
	casos.Area_Rural AS area_rural,
	casos.Corregimiento AS corregimiento,
	casos.Vereda AS vereda,
	casos.CódigoCentroPoblado AS codigo_centro_poblado,
	casos.CentroPoblado AS centro_poblado,
	casos.TipoCentroPoblado AS tipo_centro_poblado,
	casos.SITIO AS sitio,
	casos.Territorio_Colectivo AS territorio_colectivo,
	casos.Resguardo AS resguardo,
	casos.Modalidad AS modalidad,
	casos.ModalidadDescripcion AS modalidad_descripcion,
	casos.FormaVinculacion AS forma_vinculacion,
	casos.TipoVinculacion AS tipo_vinculacion,
	casos.PorteListas AS porte_listas,
	casos.IngresoViviendaFinca AS ingreso_vivienda_finca,
	casos.Encapuchados AS encapuchados,
	casos.PerpetradorIdentificado AS perpetrador_identificado,
	casos.IngresoEscuela AS ingreso_escuela,
	casos.PRESUNTO_REPONSABLE AS presunto_reponsable,
	casos.DESCRIPCION_PRESUNTO_RESPONSABLE1 AS descripcion_presunto_responsable1,
	casos.EspeficicacionPresuntoResponsable AS espeficicacion_presunto_responsable_2,
	casos.Observaciones_Presunto_Responsable1 AS observaciones_presunto_responsable1,
	casos.NumeroCombatientesGrupoArmado1 AS numero_combatientes_grupo_armado1,
	casos.DescripcionCombatientesGrupoArmado1 as descripcion_combatientes_grupo_armado1,
	casos.ArmasGrupoArmado1 AS armas_grupo_armado1,
	casos.DescripcionTipoArmasGrupoArmado1 AS descripcion_tipo_armas_grupo_armado1,
	casos.AbandonoDespojoForzadoTierras AS abandono_despojo_forzado_tierras,
	casos.AmenazaIntimidacion AS amenaza_intimidacion,
	casos.AtaqueContraMisionMedica AS ataque_contra_mision_medica,
	casos.ConfinamientoRestriccionMovilidad AS confinamiento_restriccion_movilidad,
	casos.DesplazamientoForzado AS desplazamiento_forzado,
	casos.Extorsion AS extorsion,
	casos.LesionadosCiviles AS lesionados_civiles,
	casos.Pillaje AS pillaje,
	casos.Tortura AS tortura,
	casos.ViolenciaBasadaGenero AS violencia_basada_genero,
	casos.OtroHechoSimultaneo AS otro_hecho_simultaneo,
	casos.GrafitisLetreros AS grafitis_letreros,
	casos.VinculosFamiliares AS vinculos_familiares,
	casos.MujeresEmbarazadas AS mujeres_embarazadas,
	casos.DescripcionDelCaso AS descripcion_del_caso,
	casos.Usuario AS usuario,
	casos.Estado AS estado_2, 
	casos.IdentificadorCaso AS identificador_caso_2,
	casos.TipoCaso AS tipo_caso,
	casos.CasoMaestro AS caso_maestro,
	casos.NumeroVictimasCaso as numero_victimas_caso
  FROM [dbo].[V_CNMH_RU] personas 
		LEFT JOIN [dbo].[V_CNMH_RU_C] casos ON casos.IdCaso = personas.IdCaso
END
GO


