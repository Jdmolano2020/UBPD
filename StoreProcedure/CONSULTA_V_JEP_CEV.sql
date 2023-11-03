USE [ubpd_base]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_JEP_CEV]    Script Date: 3/11/2023 9:58:17 a. m. ******/
DROP PROCEDURE [dbo].[CONSULTA_V_JEP_CEV]
GO

/****** Object:  StoredProcedure [dbo].[CONSULTA_V_JEP_CEV]    Script Date: 3/11/2023 9:58:17 a. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




-- =============================================
-- Author:		Juan Daniel Molano Castro
-- Create date: 17/10/2023
-- Description:	Realiza la consulta de las personas desaparecidas de la fuente JEP_CEV
-- =============================================
CREATE   PROCEDURE [dbo].[CONSULTA_V_JEP_CEV]
WITH RECOMPILE 
AS
BEGIN
  SET NOCOUNT ON;
  SELECT 
	personas.match_group_id AS codigo_unico_fuente,
	personas.cedula,
	personas.otro_documento,
	personas.nombre_apellido_completo,
	personas.nombre_1,
	personas.nombre_2,
	personas.apellido_1,
	personas.apellido_2,
	personas.edad ,
	personas.yy_nacimiento,
	personas.mm_nacimiento,
	personas.dd_nacimiento,
	personas.sexo,
	personas.etnia,
	personas.dept_code_hecho,
	personas.muni_code_hecho,
	personas.yy_hecho,
	personas.ymd_hecho,
	personas.tipohecho,
	personas.yy_nacimiento,
	personas.yy_nacimiento,
	personas.yy_nacimiento,
	personas.yy_nacimiento,
	personas.perp_agentes_estatales,
    personas.perp_grupos_posdesmv_paramilitar,
    personas.perp_paramilitares,
    personas.perp_guerrilla_eln,
    personas.perp_guerrilla_farc,
    personas.perp_guerrilla_otra,
    personas.perp_otro,
	personas.perp_guerrilla,
    personas.in_RUV,
    personas.in_VP_DAS,
    personas.in_URT,
    personas.in_UPH,
    personas.in_UP,
    personas.in_SINDICALISTAS,
    personas.in_SIJYP,
    personas.in_PONAL,
    personas.in_PGN,
    personas.in_PERSONERIA,
    personas.in_PAISLIBRE,
    personas.in_ONIC,
    personas.in_OACP,
    personas.in_MINDEFENSA,
    personas.in_JMP,
    personas.in_INML,
    personas.in_ICBF,
    personas.in_FORJANDOFUTUROS,
    personas.in_FGN,
    personas.in_EJERCITO,
    personas.in_CREDHOS,
    personas.in_CONASE,
    personas.in_CNMH,
    personas.in_COMUNIDADES_NEGRAS,
    personas.in_CEV,
    personas.in_CECOIN,
    personas.in_CCJ,
    personas.in_CCEEU,
    personas.in_CARIBE,
	personas.narrativo_hechos
	  FROM [ORQ_SALIDA].[CONSOLIDADO_JEP] personas   
  END
GO


