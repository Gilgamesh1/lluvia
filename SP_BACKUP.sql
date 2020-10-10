create or replace PACKAGE BODY PPYM_REPORT AS

PROCEDURE SP_EXEC_REPORT_MOTOR(REPORT_NAME IN VARCHAR2, COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2, O_RESULT OUT INT, O_MENSAJE OUT VARCHAR2)
AS
TYPE TT_LST_TABLA_FLUJO IS RECORD (
        id_flujo      NUMBER,
        codigo_flujo      VARCHAR2(100)
        );
TYPE RT is table of TT_LST_TABLA_FLUJO;
listaFlujo  RT;
O_RESULT_S INT;
O_MENSAJE_S VARCHAR2(500);

STMT            VARCHAR2(200);
FLG_REPORT      VARCHAR2(10);
FECHA_FINAL_MOI VARCHAR2(10);
FEC_LAST_LOAD_MOI VARCHAR2(10);
COD_AAP_14000001 VARCHAR2(10);
BEGIN
   SP_WRITE_LOG('');
   SP_WRITE_LOG('Ini SP_EXEC_REPORT_MOTOR');
   O_RESULT_S := 0;
   O_MENSAJE_S := '';

   STMT := 'SELECT NO_DESC_VAL FLG_R FROM MOTEVA.MER_CAT_VARIABLE WHERE co_variable = ''QRTZ_REPORT_TRAZABILIDAD''';
   EXECUTE IMMEDIATE (STMT) INTO FLG_REPORT;

   IF FLG_REPORT = 0 THEN
     --bloqueo
     update MOTEVA.MER_CAT_VARIABLE set NO_DESC_VAL='1'WHERE co_variable = 'QRTZ_REPORT_TRAZABILIDAD';
     commit;

     SELECT id_flujo, codigo_flujo
     BULK COLLECT INTO listaFlujo
     FROM (
        select a.id_flujo,a.codigo_flujo
        from moteva.mer_det_flujo a
        left join moteva.mer_cat_canal b on a.id_canal = b.id_canal
        left join moteva.mer_cat_producto c on a.id_producto = c.id_producto
     ) T;

     SP_WRITE_LOG('   REPORT_NAME=' || REPORT_NAME);
     SP_WRITE_LOG('   COD_PERIODO_INI=' || COD_PERIODO_INI);
     SP_WRITE_LOG('   COD_PERIODO_FIN=' || COD_PERIODO_FIN);

     STMT := 'SELECT NO_DESC_VAL FROM MOTEVA.MER_CAT_VARIABLE WHERE co_variable = ''COD_AAP_14000001''';
     EXECUTE IMMEDIATE (STMT) INTO COD_AAP_14000001;
     IF listaFlujo.COUNT > 0 THEN
       FOR i IN listaFlujo.FIRST .. listaFlujo.LAST LOOP
       BEGIN
         SP_WRITE_LOG('Reporte ' || listaFlujo(i).codigo_flujo);

         CASE LOWER(REPORT_NAME)
         WHEN 'evaluacion' THEN
            IF listaFlujo(i).codigo_flujo <> COD_AAP_14000001 THEN
               SP_DL_REPORT_DASHBOARD(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo,'N', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_DASHBOARD Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
               O_RESULT  := O_RESULT_S;
               O_MENSAJE := O_MENSAJE_S;
            ELSE
               SP_DL_REPORT_DIARIO_RIESGOS(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo,'N', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_DIARIO_RIESGOS Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);

               SP_DL_REPORT_NOMINAS(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, 'D01','D', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_NOMINAS DIA Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
               O_RESULT  := O_RESULT_S;
               O_MENSAJE := O_MENSAJE_S;

               STMT := 'SELECT NO_DESC_VAL FROM MOTEVA.MER_CAT_VARIABLE WHERE co_variable = ''NOMINA_LAST_MOI''';
               EXECUTE IMMEDIATE (STMT) INTO FEC_LAST_LOAD_MOI;
               --CONSULTAMOS LA TABLA MER_EXT_REPORTE_MOI PARA OBTENER LA FECHA DE CREACION Y SOBREESCRIBIR LA VARIABLE COD_PERIODO_FIN
               STMT := 'SELECT TO_CHAR(FECHA_CREACION,''YYYYMMDD'') FROM MOTEVA.MER_EXT_REPORTE_MOI WHERE ROWNUM = 1';
               EXECUTE IMMEDIATE (STMT) INTO FECHA_FINAL_MOI;

               IF FECHA_FINAL_MOI>FEC_LAST_LOAD_MOI AND trunc(sysdate - TO_DATE(FECHA_FINAL_MOI,'YYYYMMDD'))=1 THEN
                  SP_DL_REPORT_NOMINAS(TO_CHAR(TO_DATE(FEC_LAST_LOAD_MOI,'YYYYMMDD')+1,'YYYYMMDD'), FECHA_FINAL_MOI, listaFlujo(i).id_flujo, 'M01','M', O_RESULT_S, O_MENSAJE_S);
                  SP_WRITE_LOG('REPORT_NOMINAS MES Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
                  O_RESULT  := O_RESULT_S;
                  O_MENSAJE := O_MENSAJE_S;
                  IF O_RESULT = 1 THEN
                     update MOTEVA.MER_CAT_VARIABLE set NO_DESC_VAL=FECHA_FINAL_MOI WHERE co_variable = 'NOMINA_LAST_MOI';
                     commit;
                  END IF;
               END IF;
            END IF;
         WHEN 'online' THEN
            IF listaFlujo(i).codigo_flujo <> COD_AAP_14000001 THEN
               SP_DL_REPORT_DASHBOARD(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo,'N', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_DASHBOARD Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
               O_RESULT  := O_RESULT_S;
               O_MENSAJE := O_MENSAJE_S;
            ELSE
               SP_DL_REPORT_DIARIO_RIESGOS(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo,'N', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_DIARIO_RIESGOS Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
               O_RESULT  := O_RESULT_S;
               O_MENSAJE := O_MENSAJE_S;
            END IF;
         WHEN 'batchdia' THEN
            IF listaFlujo(i).codigo_flujo = COD_AAP_14000001 THEN
               SP_DL_REPORT_NOMINAS(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, 'D01','D', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_NOMINAS DIA Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
               O_RESULT  := O_RESULT_S;
               O_MENSAJE := O_MENSAJE_S;
            END IF;
         WHEN 'batchmes' THEN
            IF listaFlujo(i).codigo_flujo = COD_AAP_14000001 THEN
               STMT := 'SELECT NO_DESC_VAL FROM MOTEVA.MER_CAT_VARIABLE WHERE co_variable = ''NOMINA_LAST_MOI''';
               EXECUTE IMMEDIATE (STMT) INTO FEC_LAST_LOAD_MOI;
               --CONSULTAMOS LA TABLA MER_EXT_REPORTE_MOI PARA OBTENER LA FECHA DE CREACION Y SOBREESCRIBIR LA VARIABLE COD_PERIODO_FIN
               STMT := 'SELECT TO_CHAR(FECHA_CREACION,''YYYYMMDD'') FROM MOTEVA.MER_EXT_REPORTE_MOI WHERE ROWNUM = 1';
               EXECUTE IMMEDIATE (STMT) INTO FECHA_FINAL_MOI;

               IF FECHA_FINAL_MOI>FEC_LAST_LOAD_MOI AND trunc(sysdate - TO_DATE(FECHA_FINAL_MOI,'YYYYMMDD'))=1 THEN
                  SP_DL_REPORT_NOMINAS(TO_CHAR(TO_DATE(FEC_LAST_LOAD_MOI,'YYYYMMDD')+1,'YYYYMMDD'), FECHA_FINAL_MOI, listaFlujo(i).id_flujo, 'M01','M', O_RESULT_S, O_MENSAJE_S);
                  SP_WRITE_LOG('REPORT_NOMINAS MES Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
                  O_RESULT  := O_RESULT_S;
                  O_MENSAJE := O_MENSAJE_S;
                  IF O_RESULT = 1 THEN
                     update MOTEVA.MER_CAT_VARIABLE set NO_DESC_VAL=FECHA_FINAL_MOI WHERE co_variable = 'NOMINA_LAST_MOI';
                     commit;
                  END IF;
               END IF;
            END IF;
         WHEN 'error' THEN
            SP_DL_REPORT_DASHBOARD(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo,'E', O_RESULT_S, O_MENSAJE_S);
            O_RESULT  := O_RESULT_S;
            O_MENSAJE := O_MENSAJE_S;
         ELSE
            SP_WRITE_LOG('SP_EXEC_REPORT_MOTOR-INVALID_REPORT_NAME: Nombre del reporte no valido');
            O_RESULT  := 99;
            O_MENSAJE := ' ERROR - INVALID_REPORT_NAME';
         END CASE;
       END;
       END LOOP;
     END IF;
     --desbloqueo
     update MOTEVA.MER_CAT_VARIABLE set NO_DESC_VAL='0'WHERE co_variable = 'QRTZ_REPORT_TRAZABILIDAD';
     commit;
   ELSE
     SP_WRITE_LOG('Reporte bloqueado');
   END IF;
   SP_WRITE_LOG('Fin SP_EXEC_REPORT_MOTOR');

END SP_EXEC_REPORT_MOTOR;

PROCEDURE SP_EXEC_REPORT_MOTOR(REPORT_NAME IN VARCHAR2, COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2)
AS
TYPE TT_LST_TABLA_FLUJO IS RECORD (
        id_flujo      NUMBER,
        codigo_flujo      VARCHAR2(100)
        );
TYPE RT is table of TT_LST_TABLA_FLUJO;
listaFlujo  RT;

O_RESULT_S INT;
O_MENSAJE_S VARCHAR2(500);
STMT            VARCHAR2(200);
FECHA_FINAL_MOI VARCHAR2(10);
FEC_LAST_LOAD_MOI VARCHAR2(10);
COD_AAP_14000001 VARCHAR2(10);
BEGIN
   SP_WRITE_LOG('');
   SP_WRITE_LOG('Ini SP_EXEC_REPORT_MOTOR - contingencia call');
   O_RESULT_S := 0;
   O_MENSAJE_S := '';

   SELECT id_flujo, codigo_flujo
   BULK COLLECT INTO listaFlujo
   FROM (
      select a.id_flujo,a.codigo_flujo
      from moteva.mer_det_flujo a
      left join moteva.mer_cat_canal b on a.id_canal = b.id_canal
      left join moteva.mer_cat_producto c on a.id_producto = c.id_producto
   ) T;

   SP_WRITE_LOG('   REPORT_NAME=' || REPORT_NAME);
   SP_WRITE_LOG('   COD_PERIODO_INI=' || COD_PERIODO_INI);
   SP_WRITE_LOG('   COD_PERIODO_FIN=' || COD_PERIODO_FIN);

   --netkinzi ini
   IF LOWER(REPORT_NAME) = 'crediticia' THEN
      SP_DL_REPORT_EV_CREDITICIA(COD_PERIODO_INI, COD_PERIODO_FIN, O_RESULT_S, O_MENSAJE_S);
      SP_WRITE_LOG('SP_DL_REPORT_EV_CREDITICIA-EXECUTE: ');
      SP_WRITE_LOG('Return Code: '|| O_RESULT_S);
      SP_WRITE_LOG('Mensaje: '|| O_MENSAJE_S);
   END IF;
   --netkinzi fin

   STMT := 'SELECT NO_DESC_VAL FROM MOTEVA.MER_CAT_VARIABLE WHERE co_variable = ''COD_AAP_14000001''';
   EXECUTE IMMEDIATE (STMT) INTO COD_AAP_14000001;

   IF listaFlujo.COUNT > 0 THEN
     FOR i IN listaFlujo.FIRST .. listaFlujo.LAST LOOP
     BEGIN
       SP_WRITE_LOG('Reporte ' || listaFlujo(i).codigo_flujo);

       CASE LOWER(REPORT_NAME)
       WHEN 'evaluacion' THEN
          IF listaFlujo(i).codigo_flujo <> COD_AAP_14000001 THEN
              SP_DL_REPORT_DASHBOARD(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo, 'N', O_RESULT_S, O_MENSAJE_S);
              SP_WRITE_LOG('REPORT_DASHBOARD Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
          ELSE
              SP_DL_REPORT_DIARIO_RIESGOS(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo,'N', O_RESULT_S, O_MENSAJE_S);
              SP_WRITE_LOG('REPORT_DIARIO_RIESGOS Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);

              SP_DL_REPORT_NOMINAS(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, 'D01','D', O_RESULT_S, O_MENSAJE_S);
              SP_WRITE_LOG('REPORT_NOMINAS DIA Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);

              STMT := 'SELECT NO_DESC_VAL FROM MOTEVA.MER_CAT_VARIABLE WHERE co_variable = ''NOMINA_LAST_MOI''';
              EXECUTE IMMEDIATE (STMT) INTO FEC_LAST_LOAD_MOI;
              --CONSULTAMOS LA TABLA MER_EXT_REPORTE_MOI PARA OBTENER LA FECHA DE CREACION Y SOBREESCRIBIR LA VARIABLE COD_PERIODO_FIN
              STMT := 'SELECT TO_CHAR(FECHA_CREACION,''YYYYMMDD'') FROM MOTEVA.MER_EXT_REPORTE_MOI WHERE ROWNUM = 1';
              EXECUTE IMMEDIATE (STMT) INTO FECHA_FINAL_MOI;

              IF FECHA_FINAL_MOI>FEC_LAST_LOAD_MOI AND trunc(sysdate - TO_DATE(FECHA_FINAL_MOI,'YYYYMMDD'))=1 THEN
                 SP_DL_REPORT_NOMINAS(TO_CHAR(TO_DATE(FEC_LAST_LOAD_MOI,'YYYYMMDD')+1,'YYYYMMDD'), FECHA_FINAL_MOI, listaFlujo(i).id_flujo, 'M01','M', O_RESULT_S, O_MENSAJE_S);
                 SP_WRITE_LOG('REPORT_NOMINAS MES Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
                 IF O_RESULT_S = 1 THEN
                    update MOTEVA.MER_CAT_VARIABLE set NO_DESC_VAL=FECHA_FINAL_MOI WHERE co_variable = 'NOMINA_LAST_MOI';
                    commit;
                 END IF;
              END IF;
          END IF;
       WHEN 'online' THEN
            IF listaFlujo(i).codigo_flujo <> COD_AAP_14000001 THEN
               SP_DL_REPORT_DASHBOARD(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo,'N', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_DASHBOARD Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
            ELSE
               SP_DL_REPORT_DIARIO_RIESGOS(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo,'N', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_DIARIO_RIESGOS Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
            END IF;
       WHEN 'batchdia' THEN
            IF listaFlujo(i).codigo_flujo = COD_AAP_14000001 THEN
               SP_DL_REPORT_NOMINAS(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, 'D01','D', O_RESULT_S, O_MENSAJE_S);
               SP_WRITE_LOG('REPORT_NOMINAS DIA Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
            END IF;
       WHEN 'batchmes' THEN
            IF listaFlujo(i).codigo_flujo = COD_AAP_14000001 THEN
               STMT := 'SELECT NO_DESC_VAL FROM MOTEVA.MER_CAT_VARIABLE WHERE co_variable = ''NOMINA_LAST_MOI''';
               EXECUTE IMMEDIATE (STMT) INTO FEC_LAST_LOAD_MOI;
               --CONSULTAMOS LA TABLA MER_EXT_REPORTE_MOI PARA OBTENER LA FECHA DE CREACION Y SOBREESCRIBIR LA VARIABLE COD_PERIODO_FIN
               STMT := 'SELECT TO_CHAR(FECHA_CREACION,''YYYYMMDD'') FROM MOTEVA.MER_EXT_REPORTE_MOI WHERE ROWNUM = 1';
               EXECUTE IMMEDIATE (STMT) INTO FECHA_FINAL_MOI;

               IF FECHA_FINAL_MOI>FEC_LAST_LOAD_MOI AND trunc(sysdate - TO_DATE(FECHA_FINAL_MOI,'YYYYMMDD'))=1 THEN
                  SP_DL_REPORT_NOMINAS(TO_CHAR(TO_DATE(FEC_LAST_LOAD_MOI,'YYYYMMDD')+1,'YYYYMMDD'), FECHA_FINAL_MOI, listaFlujo(i).id_flujo, 'M01','M', O_RESULT_S, O_MENSAJE_S);
                  SP_WRITE_LOG('REPORT_NOMINAS MES Return Code: '|| O_RESULT_S || ' Mensaje: '|| O_MENSAJE_S);
                  IF O_RESULT_S = 1 THEN
                     update MOTEVA.MER_CAT_VARIABLE set NO_DESC_VAL=FECHA_FINAL_MOI WHERE co_variable = 'NOMINA_LAST_MOI';
                     commit;
                  END IF;
               END IF;
            END IF;
       WHEN 'error' THEN
          SP_DL_REPORT_DASHBOARD(COD_PERIODO_INI, COD_PERIODO_FIN, listaFlujo(i).id_flujo, listaFlujo(i).codigo_flujo, 'E', O_RESULT_S, O_MENSAJE_S);
          SP_WRITE_LOG('SP_EXEC_REPORT_MOTOR-EXECUTE: ');
          SP_WRITE_LOG('Return Code: '|| O_RESULT_S);
          SP_WRITE_LOG('Mensaje: '|| O_MENSAJE_S);
       ELSE
          SP_WRITE_LOG('SP_EXEC_REPORT_MOTOR-INVALID_REPORT_NAME: Nombre del reporte no valido');
       END CASE;
     END;

     END LOOP;
   END IF;

   SP_WRITE_LOG('Fin SP_EXEC_REPORT_MOTOR');
END SP_EXEC_REPORT_MOTOR;

PROCEDURE SP_PROC_REPORT_DASHBOARD(COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2, ID_FLUJO NUMBER, DICTAMEN VARCHAR2, P_RPT OUT SYS_REFCURSOR, return_code OUT NUMBER)
as
v_sql VARCHAR2(30000);
begin
  SP_WRITE_LOG('');
  SP_WRITE_LOG('Ini SP_PROC_REPORT_DASHBOARD');
  return_code := 0;

  v_sql := 'select *
            from (
              select di.id_evaluacion as id_evaluacion,
                   to_char(rec.fe_usua_crea,''DD/MM/YYYY HH24:MI:SS AM'') as fecha_hora,
                   to_char(rec.fe_usua_crea,''DD/MM/YYYY'') as fecha,
                   to_char(rec.fe_usua_crea,''HH24:MI:SS AM'') as hora,
                   rec.session_id id_session,
                   cli.co_tipo_documento as tip_doc,
                   cli.de_documento as nro_doc,
                   cli.co_central as cod_central,
                   rec.co_tarea as ult_tarea,
                   rec.co_paso  as ult_paso,
                   rec.intentos as ult_intentos,
                   rec.de_rechazo as ult_rechazo,
                   rec.in_resultado as ult_resultado,
                   mcc.co_canal || ''-'' || mcc.de_canal as canal,
                   cev.co_subcanal as subcanal,
                   mcp.co_producto || ''-'' || mcp.de_producto as producto,
                   cev.co_subproducto as subproducto,
                   mdf.codigo_flujo,
                   di.param_value,
                   lpad(mdt.id_tarea,3,''0'')||lpad(mcf.id_funcion,3,''0'')||lpad(di.id_param,4,''0'') campo
            from moteva.mer_det_informacion di
            inner join moteva.mer_cat_cliente cli on di.id_cliente = cli.id_cliente
            inner join moteva.mer_cat_evaluacion cev on di.id_evaluacion = cev.id_evaluacion
            inner join moteva.mer_rel_eval_cliente rec on di.id_cliente = rec.id_cliente and di.id_evaluacion = rec.id_evaluacion
            left join moteva.mer_det_tarea mdt on di.codigo_tarea = mdt.co_tarea and mdt.id_flujo='||ID_FLUJO||'
            left join moteva.mer_cat_funcion mcf on di.codigo_funcion = mcf.co_funcion
            left join moteva.mer_cat_canal mcc on cev.co_canal = mcc.co_canal
            left join moteva.mer_cat_producto mcp on cev.co_producto = mcp.co_producto
            left join (
              select h1.id_flujo, h2.co_canal, h2.de_canal, h3.co_producto, h3.de_producto, h1.codigo_flujo, h4.co_canal co_subcanal, h5.co_producto co_subproducto
              from moteva.mer_det_flujo h1
              left join moteva.mer_cat_canal h2 on h1.id_canal=h2.id_canal
              left join moteva.mer_cat_canal h4 on h1.id_subcanal = h4.id_canal
              left join moteva.mer_cat_producto h3 on h1.id_producto = h3.id_producto
              left join moteva.mer_cat_producto h5 on h1.id_subproducto = h5.id_producto
            ) mdf on cev.co_canal = mdf.co_canal and cev.co_producto =mdf.co_producto
            and cev.co_subcanal = mdf.co_subcanal and (nvl(cev.co_subproducto,''xxxx'')=nvl(mdf.co_subproducto,''xxxx''))
            and mdf.id_flujo='||ID_FLUJO||'
            where mdf.id_flujo is not null and rec.IN_RESULTADO IN ('''||DICTAMEN||''') and di.codigo_funcion in
            (select f.co_funcion from moteva.mer_cat_funcion f where f.tipo_serv = ''PWC'')
            and (to_char(rec.fe_usua_crea,''YYYYMMDD'')>= '''||COD_PERIODO_INI|| '''
                and to_char(rec.fe_usua_crea,''YYYYMMDD'')<= '''||COD_PERIODO_FIN|| ''')
            )
             pivot ( min(param_value)
                    for (campo) in
                     (' || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION'))>0      then        FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX'))>0      then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS'))>0             then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS')             end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI'))>0              then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI')              end ||
                      '))
            order  by id_evaluacion' ;
  --SP_WRITE_LOG(v_sql);
  OPEN P_RPT FOR v_sql;
  SP_WRITE_LOG('Fin SP_PROC_REPORT_DASHBOARD');
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      SP_WRITE_LOG('No se encontraron registros: ' || SQLERRM);
   WHEN OTHERS THEN
      SP_WRITE_LOG('Error de base de datos: ' || SQLERRM);
END SP_PROC_REPORT_DASHBOARD;


PROCEDURE SP_DL_REPORT_DASHBOARD(COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2, ID_FLUJO NUMBER, FILE_NAME VARCHAR2, TIPO_RPT VARCHAR2, O_RESULT OUT INT, O_MENSAJE OUT VARCHAR2)
AS
    OUT_ROL_ROWS  SYS_REFCURSOR;
    OUT_FILE_NAME VARCHAR2(400):='';
    OUTPUT_FILE   UTL_FILE.FILE_TYPE;
    BUFFER        VARCHAR2(30000);
    STMT          VARCHAR2(200);
    STMH          VARCHAR2(200);
    FECHA_PROCESO VARCHAR(200);
    HORA_PROCESO  VARCHAR(200);
    ENCABEZADO    VARCHAR(3500);
    i_periodo     VARCHAR2(8);
    return_code   NUMBER;

    curid         INTEGER;
    colcnt        INTEGER:=0;
    desctab       dbms_sql.desc_tab;

    namevar    VARCHAR2 (100);
    numvar     NUMBER;
    datevar    DATE;
    row_cnt    INTEGER:=0;
    err_file_exist boolean:=false;
    err_file_len number;
    err_block_size number;

    arr_dictamen VARCHAR2(100):='';
BEGIN
    --DBMS_OUTPUT.ENABLE(buffer_size => NULL);
    SP_WRITE_LOG('');
    SP_WRITE_LOG('Ini SP_DL_REPORT_DASHBOARD');
    SP_WRITE_LOG('Exportar TEXTO REPORT_DASHBOARD');
    i_periodo :=  COD_PERIODO_INI; --FORMATO YYYYMMDD
    STMT := 'SELECT TO_CHAR(SYSDATE, ' || CHR(39) ||
            'YYYYMMDD'      || CHR(39) ||
            ') FROM DUAL';
    STMH := 'SELECT TO_CHAR(SYSDATE, ' || CHR(39) ||
            'HH24MISS'      || CHR(39) ||
            ') FROM DUAL';

    EXECUTE IMMEDIATE (STMT) INTO FECHA_PROCESO;
    EXECUTE IMMEDIATE (STMH) INTO HORA_PROCESO;

    if TIPO_RPT = 'N' then
       if (to_date(COD_PERIODO_FIN, 'YYYYMMDD') - to_date(COD_PERIODO_INI, 'YYYYMMDD')) = 0 then
          OUT_FILE_NAME := 'RPT_DASHBOARD_'||FILE_NAME||'_'||i_periodo || '.csv';
       else
          OUT_FILE_NAME := 'RPT_DASHBOARD_'||FILE_NAME||'_'||i_periodo||'_'||COD_PERIODO_FIN || '.csv';
       end if;
       SP_WRITE_LOG(OUT_FILE_NAME);
       arr_dictamen := '1'',''2'',''3';
    elsif TIPO_RPT = 'E' then
       if (to_date(COD_PERIODO_FIN, 'YYYYMMDD') - to_date(COD_PERIODO_INI, 'YYYYMMDD')) = 0 then
          OUT_FILE_NAME := 'RPT_ERROR_'||FILE_NAME||'_'||i_periodo || '.csv';
       else
          OUT_FILE_NAME := 'RPT_ERROR_'||FILE_NAME||'_'||i_periodo||'_'||COD_PERIODO_FIN || '.csv';
       end if;
       SP_WRITE_LOG(OUT_FILE_NAME);
       arr_dictamen := '9';
    else
       OUT_FILE_NAME := 'RPT_NNNNN_'||FILE_NAME||'_'||i_periodo || '.csv';
       SP_WRITE_LOG(OUT_FILE_NAME);
       arr_dictamen := '0';
    end if;

    SP_WRITE_LOG('Exe SP_PROC_REPORT_DASHBOARD');
    SP_PROC_REPORT_DASHBOARD(COD_PERIODO_INI, COD_PERIODO_FIN, ID_FLUJO, arr_dictamen, OUT_ROL_ROWS, return_code);

    curid := DBMS_SQL.to_cursor_number (OUT_ROL_ROWS);
    DBMS_SQL.describe_columns (curid , colcnt, desctab);

    --SP_WRITE_LOG('colcnt :' || colcnt);
    FOR indx IN 1 .. colcnt
    LOOP
      BUFFER := BUFFER || desctab (indx).col_name   || CHR(59);
      IF desctab (indx).col_type = 2 THEN
         DBMS_SQL.define_column (curid , indx, numvar);
      ELSIF desctab (indx).col_type = 12 THEN
         DBMS_SQL.define_column (curid , indx, datevar);
      ELSE
         DBMS_SQL.define_column (curid , indx, namevar, 100);
      END IF;
    END LOOP;

   WHILE DBMS_SQL.fetch_rows (curid) > 0
   LOOP
   BEGIN
     if row_cnt = 0 then
       if FILE_NAME = 'ERROR' then
         UTL_FILE.fgetattr('DIR_MOTOR', OUT_FILE_NAME,err_file_exist,err_file_len,err_block_size);
         if err_file_exist then
           OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'A', 32767);
         else
           OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'W', 32767);
         end if;
       else
         OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'W', 32767);
       end if;

       --SP_WRITE_LOG('columnas :' || BUFFER);
       UTL_FILE.PUT_LINE(OUTPUT_FILE, BUFFER);
     end if;

     BUFFER := '';
     FOR indx IN 1 .. colcnt
      LOOP
         --SP_WRITE_LOG(desctab (indx).col_name || ' = ');
         IF (desctab (indx).col_type = 2)
         THEN
            DBMS_SQL.COLUMN_VALUE (curid, indx, numvar);
            BUFFER := BUFFER || numvar || CHR(59);
         ELSIF (desctab (indx).col_type = 12)
         THEN
            DBMS_SQL.COLUMN_VALUE (curid, indx, datevar);
            BUFFER := BUFFER || datevar || CHR(59);
         ELSE
            DBMS_SQL.COLUMN_VALUE (curid, indx, namevar);
            BUFFER := BUFFER || namevar || CHR(59);
         END IF;
      END LOOP;
      --SP_WRITE_LOG('fila :' || BUFFER);
      UTL_FILE.PUT_LINE(OUTPUT_FILE, BUFFER);
      row_cnt := row_cnt + 1;
   EXCEPTION
      WHEN OTHERS THEN
         SP_WRITE_LOG('SP_DL_REPORT_DASHBOARD Ocurrio un error en buffer');
         SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
   END;
   END LOOP;

   if UTL_FILE.IS_OPEN(OUTPUT_FILE) then
     UTL_FILE.FFLUSH(OUTPUT_FILE);
   end if;
   DBMS_SQL.close_cursor (curid);

   SP_WRITE_LOG('Fin Loop ');

   if UTL_FILE.IS_OPEN(OUTPUT_FILE) then
     UTL_FILE.FCLOSE(OUTPUT_FILE);
   end if;
   /*if (row_cnt = 0)
   then
      --borrar el archivo dado que no tiene informacion
      SP_WRITE_LOG('borrar archivo '|| OUT_FILE_NAME);
      UTL_FILE.FREMOVE('DIR_MOTOR', OUT_FILE_NAME);
   end if;*/

   SP_WRITE_LOG('Fin Exportar TEXTO REPORT_DASHBOARD');
   SP_WRITE_LOG('Fin SP_DL_REPORT_DASHBOARD');

   O_RESULT := 1;
	 O_MENSAJE := 'ok';
EXCEPTION
    WHEN UTL_FILE.INVALID_PATH THEN
    SP_WRITE_LOG('SP_DL_REPORT_DASHBOARD-UTL_FILE.INVALID_PATH: Ocurrio un error');
    SP_WRITE_LOG('ERROR: Ruta de archivo invalida.');
    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
    O_RESULT  := 99;
  	O_MENSAJE := SQLCODE||' ERROR '||SQLERRM;
    WHEN OTHERS THEN
    SP_WRITE_LOG('SP_DL_REPORT_DASHBOARD-OTHERS: Ocurrio un error');
    SP_WRITE_LOG(SQLERRM);
    SP_WRITE_LOG(SQLCODE);
    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
    O_RESULT  := 99;
		O_MENSAJE := SQLCODE||' ERROR '||SQLERRM;
END SP_DL_REPORT_DASHBOARD;

PROCEDURE SP_DL_TB_LIGHT_REAL
AS
    TYPE TT_DETALLE_LST IS TABLE OF mer_cat_tb_light%ROWTYPE INDEX BY PLS_INTEGER;
    wt_detalle_lst          TT_DETALLE_LST;

    CURSOR OUT_ROL_ROWS IS SELECT * FROM mer_cat_tb_light;

    OUT_FILE_NAME VARCHAR2(400):='';
    OUTPUT_FILE   UTL_FILE.FILE_TYPE;
    BUFFER        VARCHAR2(30000);
    STMT          VARCHAR2(200);
    STMH          VARCHAR2(200);
    FECHA_PROCESO VARCHAR(200);
    HORA_PROCESO VARCHAR(200);
    ENCABEZADO    VARCHAR(3500);
    i_periodo       VARCHAR2(8);

    return_code NUMBER;
BEGIN

    SP_WRITE_LOG('');
    SP_WRITE_LOG('Inicio Exportar LIGHT SIMUL: '||TO_CHAR(SYSTIMESTAMP, 'DD-Mon-YYYY HH24:MI:SS.FF3'));

    --i_periodo :=  COD_PERIODO; --FORMATO YYYYMMDD
    STMT := 'SELECT TO_CHAR(SYSDATE, ' || CHR(39) ||
            'YYYYMMDD'      || CHR(39) ||
            ') FROM DUAL';
    STMH := 'SELECT TO_CHAR(SYSDATE, ' || CHR(39) ||
            'HH24MISS'      || CHR(39) ||
            ') FROM DUAL';

    EXECUTE IMMEDIATE (STMT) INTO FECHA_PROCESO;
    EXECUTE IMMEDIATE (STMH) INTO HORA_PROCESO;

    OUT_FILE_NAME := 'EXT_TB_LIGHT_ESTRATEGICO'||FECHA_PROCESO || '.txt';
    OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'W', 32767);
        SP_WRITE_LOG('Inicio: '||TO_CHAR(SYSTIMESTAMP, 'DD-Mon-YYYY HH24:MI:SS.FF3'));

    --HEADER
    BUFFER := 'TipoDeDocumento'
              || CHR(59) || 'NumeroDocumento'
              || CHR(59) || 'Codcentral'
              || CHR(59) || 'ClasificacionBbva'
              || CHR(59) || 'ClasificacionSbs'
              || CHR(59) || 'PagoPromedioMensual'
              || CHR(59) || 'ClienteIndeseado'
              || CHR(59) || 'MarcaEmpleado'
              || CHR(59) || 'MarcaCastigo'
              || CHR(59) || 'UltimaFechaCastigo'
              || CHR(59) || 'UltimoMontoCastigo'
              || CHR(59) || 'MarcaFraudePotencial'
              || CHR(59) || 'NumeroDeEntidadesConSaldo'
              || CHR(59) || 'IngresoFijoDeclarado'
              || CHR(59) || 'IngresoVariableDeclarado'
              || CHR(59) || 'MarcaImpagos'
              || CHR(59) || 'CuotaReal'
              || CHR(59) || 'FechaUltimaDeRefinanciacion'
              || CHR(59) || 'Buro'
              || CHR(59) || 'PrProactivo'
              || CHR(59) || 'CuotaProactivo'
              || CHR(59) || 'NivelDeRiesgo'
              || CHR(59) || 'ClientePeer'
              || CHR(59) || 'MarcaRefinanciado'
              || CHR(59) || 'RelevanciaPublica'
              || CHR(59) || 'TcGarantiaLiquida'
              || CHR(59) || 'MarcaCesantes'
              || CHR(59) || 'SegmentoProactivo'
              || CHR(59) || 'SaldoMedioPasivoVista'
              || CHR(59) || 'AntiguedadCliente'
              || CHR(59) || 'esCliente'
              || CHR(59) || 'DebitosEnEntidad'
              || CHR(59) || 'DomiciliaSueldo'
              || CHR(59) || 'SegmentoRiesgo'
              || CHR(59) || 'SegmentoBanco'
              || CHR(59) || 'RatioDeUsoTarjeta'
              || CHR(59) || 'PatrimonioNeto'
              || CHR(59) || 'IniFechaCastigo';
    UTL_FILE.PUT_LINE(OUTPUT_FILE, BUFFER);

    OPEN OUT_ROL_ROWS;
        LOOP
        FETCH OUT_ROL_ROWS BULK COLLECT INTO wt_detalle_lst LIMIT 2000;
            BEGIN
              IF wt_detalle_lst.COUNT > 0 THEN
                FOR i IN wt_detalle_lst.FIRST .. wt_detalle_lst.LAST LOOP
                  --DETAIL
                  BUFFER := NVL(wt_detalle_lst(i).TIP_DOC,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).DOCUMENTO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).CLIENTE,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).CLASIFICACION_BBVA,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).CLASIFICACION_SBS,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).PPM,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).CLIENTE_INDESEADO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).MARCA_EMPLEADO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).MARCA_CASTIGO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).ULT_FECHA_CASTIGO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).ULT_MONTO_CASTIGO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).FRAUDE_POTENCIAL,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).NUM_ENTIDADES_SALDO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).ING_FIJO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).ING_VAR,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).MARCA_IMPAGOS,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).CUOTA_REAL,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).FECHA_ULT_REFIN,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).BURO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).GRUPO_RIESGO_PROACTIVO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).CUOTA_PROACTIVO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).NIVEL_RIESGO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).FLG_PEER,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).MARCA_REFINANCIADO,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).RELEVANCIA_PUBLICA,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).TC_GARANTIA_LIQUIDA,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).MARCA_CESANTE,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).SEGMENTO_PROACTIVO,'')

                    || CHR(59) || NVL(wt_detalle_lst(i).saldomediopasivovista,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).antiguedadcliente,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).escliente,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).debitosenentidad,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).domiciliasueldo,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).segmentoriesgo,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).segmentobanco,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).ratiodeusotarjeta,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).patrimonioneto,'')
                    || CHR(59) || NVL(wt_detalle_lst(i).ini_fecha_castigo,'')
                    ; --NVL(wt_detalle_lst(i).SegmentoBanco,'') ;
                    UTL_FILE.PUT_LINE(OUTPUT_FILE, BUFFER);
                END LOOP;
                UTL_FILE.FFLUSH(OUTPUT_FILE);
              END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    SP_WRITE_LOG('SP_DL_TB_LIGHT_SIMUL Ocurrio un error en buffer');
                    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
            END;
    EXIT WHEN OUT_ROL_ROWS%NOTFOUND;
    END LOOP;

    SP_WRITE_LOG('Fin: '||TO_CHAR(SYSTIMESTAMP, 'DD-Mon-YYYY HH24:MI:SS.FF3'));

    UTL_FILE.FCLOSE(OUTPUT_FILE);

    SP_WRITE_LOG('Fin Exportar LIGHT SIMUL: '||TO_CHAR(SYSTIMESTAMP, 'DD-Mon-YYYY HH24:MI:SS.FF3'));

EXCEPTION
    WHEN UTL_FILE.INVALID_PATH THEN
    SP_WRITE_LOG('SP_DL_TB_LIGHT_SIMUL Ocurrio un error');
    SP_WRITE_LOG('ERROR: Ruta de archivo invalida.');
    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
    WHEN OTHERS THEN
    SP_WRITE_LOG('SP_DL_TB_LIGHT_SIMUL Ocurrio un error');
    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
END;


PROCEDURE SP_DL_REPORT_EV_CREDITICIA(

            COD_PERIODO_INI VARCHAR2,
            COD_PERIODO_FIN VARCHAR2,
            O_RESULT        OUT INT,
            O_MENSAJE       OUT VARCHAR2  )
AS
	CURSOR c_data IS
	select   T.ID_TREE,
             T.CO_CANAL , T.CO_PRODUCTO, T.ID_EVALUACION, T.CO_TAREA,
             T.TREE_TYPE, T.OBJECT_DESC_TYPE, T.TREE_CODE , T.SCORE,
            LISTAGG (T.RESULTADO, ', ') WITHIN GROUP (ORDER BY T.RESULTADO) "RESULTADO"
        from (
            select distinct
            mtdt.ID_TREE,
            mctr.ID_EVALUACION, mevl.CO_CANAL , mevl.CO_PRODUCTO,mdtr.CO_TAREA,
            mtdt.TREE_TYPE, mtdt.OBJECT_DESC_TYPE, mtdt.TREE_CODE , mtdt.SCORE ,
            '"'|| mdis.ELEMENT || '","'|| mdis.VALUE ||'","'|| mdis.OUTCOME ||'","'|| mdis.PARTIALSCORE ||'"'  as RESULTADO
            from MER_TREE_DETAIL mtdt
            inner join MER_DET_INFO_SCORECARD mdis
            on mdis.ID_TREE = mtdt.ID_TREE
            inner join MER_DET_TRAZABILIDAD mdtr
            on mdtr.ID_DET_TRAZABILIDAD = mtdt.ID_DET_TRAZABILIDAD
            inner join MER_CAT_TRAZABILIDAD mctr
            on mctr.ID_TRAZABILIDAD = mdtr.ID_TRAZABILIDAD
            inner join MER_CAT_EVALUACION mevl
            on mevl.ID_EVALUACION = mctr.ID_EVALUACION
            and (mctr.FE_USUA_CREA BETWEEN to_date(COD_PERIODO_INI ,'DD-MM-YYYY')
            and to_date(COD_PERIODO_FIN ,'DD-MM-YYYY')+1)
        ) T
        GROUP BY T.ID_TREE,T.ID_EVALUACION, T.CO_CANAL , T.CO_PRODUCTO,T.CO_TAREA,
        T.TREE_TYPE, T.OBJECT_DESC_TYPE, T.TREE_CODE , T.SCORE;

	l_refcursor   SYS_REFCURSOR;
	v_file        UTL_FILE.FILE_TYPE;
    BUFFER        VARCHAR2(30000);
    nombre_reporte VARCHAR2(500);
BEGIN
    SP_WRITE_LOG('');
    SP_WRITE_LOG('Inicio Exportar Reporte Ev-Crediticia: '||TO_CHAR(SYSTIMESTAMP, 'DD-Mon-YYYY HH24:MI:SS.FF3'));

    nombre_reporte := 'RPT_DASHBOARD_EC_'|| to_char(to_date(COD_PERIODO_INI,'DD/MM/YYYY'),'YYYYMMDD')  ||'.csv';
    SP_WRITE_LOG('Nombre archivo+: ' || nombre_reporte);

	v_file := UTL_FILE.FOPEN(location     =>   'DIR_MOTOR',
                             filename     =>   nombre_reporte,
                             open_mode    =>   'w');


	BUFFER := 'Canal'
				|| CHR(44) || 'Producto'
				|| CHR(44) || 'id_evaluacion'
				|| CHR(44) || 'Funcion'
				|| CHR(44) || 'TREE_TYPE'
				|| CHR(44) || 'OBJECT_DESC_TYPE'
				|| CHR(44) || 'TREE_CODE'
				|| CHR(44) || 'SCORE'
				|| CHR(44) || 'ELEMENT_01'
				|| CHR(44) || 'VALUE_01'
				|| CHR(44) || 'OUTCOME_01'
				|| CHR(44) || 'PARTIALSCORE_01'
				|| CHR(44) || 'ELEMENT_02'
				|| CHR(44) || 'VALUE_02'
				|| CHR(44) || 'OUTCOME_02'
				|| CHR(44) || 'PARTIALSCORE_02'
				|| CHR(44) || 'ELEMENT_03'
				|| CHR(44) || 'VALUE_03'
				|| CHR(44) || 'OUTCOME_03'
				|| CHR(44) || 'PARTIALSCORE_03'
				|| CHR(44) || 'ELEMENT_04'
				|| CHR(44) || 'VALUE_04'
				|| CHR(44) || 'OUTCOME_04'
				|| CHR(44) || 'PARTIALSCORE_04'
				|| CHR(44) || 'ELEMENT_05'
				|| CHR(44) || 'VALUE_05'
				|| CHR(44) || 'OUTCOME_05'
				|| CHR(44) || 'PARTIALSCORE_05'
				|| CHR(44) || 'ELEMENT_06'
				|| CHR(44) || 'VALUE_06'
				|| CHR(44) || 'OUTCOME_06'
				|| CHR(44) || 'PARTIALSCORE_06'
				|| CHR(44) || 'ELEMENT_07'
				|| CHR(44) || 'VALUE_07'
				|| CHR(44) || 'OUTCOME_07'
				|| CHR(44) || 'PARTIALSCORE_07'
				|| CHR(44) || 'ELEMENT_08'
				|| CHR(44) || 'VALUE_08'
				|| CHR(44) || 'OUTCOME_08'
				|| CHR(44) || 'PARTIALSCORE_08'
				|| CHR(44) || 'ELEMENT_09'
				|| CHR(44) || 'VALUE_09'
				|| CHR(44) || 'OUTCOME_09'
				|| CHR(44) || 'PARTIALSCORE_09'
				|| CHR(44) || 'ELEMENT_10'
				|| CHR(44) || 'VALUE_10'
				|| CHR(44) || 'OUTCOME_10'
				|| CHR(44) || 'PARTIALSCORE_10'
				|| CHR(44) || 'ELEMENT_11'
				|| CHR(44) || 'VALUE_11'
				|| CHR(44) || 'OUTCOME_11'
				|| CHR(44) || 'PARTIALSCORE_11'
				|| CHR(44) || 'ELEMENT_12'
				|| CHR(44) || 'VALUE_12'
				|| CHR(44) || 'OUTCOME_12'
				|| CHR(44) || 'PARTIALSCORE_12'
				|| CHR(44) || 'ELEMENT_13'
				|| CHR(44) || 'VALUE_13'
				|| CHR(44) || 'OUTCOME_13'
				|| CHR(44) || 'PARTIALSCORE_13'
				|| CHR(44) || 'ELEMENT_14'
				|| CHR(44) || 'VALUE_14'
				|| CHR(44) || 'OUTCOME_14'
				|| CHR(44) || 'PARTIALSCORE_14'
				|| CHR(44) || 'ELEMENT_15'
				|| CHR(44) || 'VALUE_15'
				|| CHR(44) || 'OUTCOME_15'
				|| CHR(44) || 'PARTIALSCORE_15'
				|| CHR(44) || 'ELEMENT_16'
				|| CHR(44) || 'VALUE_16'
				|| CHR(44) || 'OUTCOME_16'
				|| CHR(44) || 'PARTIALSCORE_16'
				|| CHR(44) || 'ELEMENT_17'
				|| CHR(44) || 'VALUE_17'
				|| CHR(44) || 'OUTCOME_17'
				|| CHR(44) || 'PARTIALSCORE_17'
				|| CHR(44) || 'ELEMENT_18'
				|| CHR(44) || 'VALUE_18'
				|| CHR(44) || 'OUTCOME_18'
				|| CHR(44) || 'PARTIALSCORE_18'
				|| CHR(44) || 'ELEMENT_19'
				|| CHR(44) || 'VALUE_19'
				|| CHR(44) || 'OUTCOME_19'
				|| CHR(44) || 'PARTIALSCORE_19'
				|| CHR(44) || 'ELEMENT_20'
				|| CHR(44) || 'VALUE_20'
				|| CHR(44) || 'OUTCOME_20'
				|| CHR(44) || 'PARTIALSCORE_20';
    UTL_FILE.PUT_LINE(v_file, BUFFER);

	FOR cur_rec IN c_data LOOP
    UTL_FILE.PUT_LINE(v_file,
					  cur_rec.CO_CANAL || ',' ||
					  cur_rec.CO_PRODUCTO || ',' ||
                      cur_rec.ID_EVALUACION || ',' ||
					  cur_rec.CO_TAREA || ',' ||
					  cur_rec.TREE_TYPE || ',' ||
					  cur_rec.OBJECT_DESC_TYPE || ',' ||
					  cur_rec.TREE_CODE || ',' ||
					  cur_rec.SCORE || ',' ||
					  cur_rec.RESULTADO
					  );
     END LOOP;

      UTL_FILE.FCLOSE(v_file);
      SP_WRITE_LOG('Fin SP_DL_REPORT_EV_CREDITICIA:  ' || 'REPORTE_EV_CREDITICIA_'|| REPLACE(COD_PERIODO_INI, '/', '-')  ||'.csv' ||TO_CHAR(SYSTIMESTAMP, 'DD-Mon-YYYY HH24:MI:SS.FF3'));
      O_RESULT := 1;
      O_MENSAJE := 'Reporte de Ev. Crediticia Generado correctamente: ' || nombre_reporte  ||'.csv';
  EXCEPTION
	  WHEN OTHERS THEN
        SP_WRITE_LOG('SP_DL_REPORT_EV_CREDITICIA Ocurrio un error');
        SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
        O_RESULT  := 99;
		O_MENSAJE := SQLCODE||' ERROR '||SQLERRM;
		UTL_FILE.FCLOSE(v_file);
		RAISE;

END SP_DL_REPORT_EV_CREDITICIA;

PROCEDURE SP_DL_REPORT_DIARIO_RIESGOS(COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2, ID_FLUJO NUMBER, FILE_NAME VARCHAR2, TIPO_RPT VARCHAR2, O_RESULT OUT INT, O_MENSAJE OUT VARCHAR2)
AS
    OUT_ROL_ROWS  SYS_REFCURSOR;
    OUT_FILE_NAME VARCHAR2(400):='';
    OUTPUT_FILE   UTL_FILE.FILE_TYPE;
    BUFFER        VARCHAR2(30000);
    STMT          VARCHAR2(200);
    STMH          VARCHAR2(200);
    FECHA_PROCESO VARCHAR(200);
    HORA_PROCESO  VARCHAR(200);
    ENCABEZADO    VARCHAR(3500);
    i_periodo     VARCHAR2(8);
    return_code   NUMBER;

    curid         INTEGER;
    colcnt        INTEGER:=0;
    desctab       dbms_sql.desc_tab;

    namevar    VARCHAR2 (100);
    numvar     NUMBER;
    datevar    DATE;
    row_cnt    INTEGER:=0;
    err_file_exist boolean:=false;
    err_file_len number;
    err_block_size number;

    arr_dictamen VARCHAR2(100):='';
BEGIN
    SP_WRITE_LOG('');
    SP_WRITE_LOG('Ini SP_DL_REPORT_DIARIO_RIESGOS');
    SP_WRITE_LOG('Exportar TEXTO REPORT_DIARIO_RIESGOS');
    i_periodo :=  COD_PERIODO_INI; --FORMATO YYYYMMDD
    STMT := 'SELECT TO_CHAR(SYSDATE, ' || CHR(39) ||
            'YYYYMMDD'      || CHR(39) ||
            ') FROM DUAL';
    STMH := 'SELECT TO_CHAR(SYSDATE, ' || CHR(39) ||
            'HH24MISS'      || CHR(39) ||
            ') FROM DUAL';

    EXECUTE IMMEDIATE (STMT) INTO FECHA_PROCESO;
    EXECUTE IMMEDIATE (STMH) INTO HORA_PROCESO;

    if TIPO_RPT = 'N' then
       if (to_date(COD_PERIODO_FIN, 'YYYYMMDD') - to_date(COD_PERIODO_INI, 'YYYYMMDD')) = 0 then
          OUT_FILE_NAME := 'RPT_DASHBOARD_'||FILE_NAME||'_'||i_periodo || '.csv';
       else
          OUT_FILE_NAME := 'RPT_DASHBOARD_'||FILE_NAME||'_'||i_periodo||'_'||COD_PERIODO_FIN || '.csv';
       end if;
       SP_WRITE_LOG(OUT_FILE_NAME);
       arr_dictamen := '1'',''2'',''3';
    elsif TIPO_RPT = 'E' then
       if (to_date(COD_PERIODO_FIN, 'YYYYMMDD') - to_date(COD_PERIODO_INI, 'YYYYMMDD')) = 0 then
          OUT_FILE_NAME := 'RPT_ERROR_'||FILE_NAME||'_'||i_periodo || '.csv';
       else
          OUT_FILE_NAME := 'RPT_ERROR_'||FILE_NAME||'_'||i_periodo||'_'||COD_PERIODO_FIN || '.csv';
       end if;
       SP_WRITE_LOG(OUT_FILE_NAME);
       arr_dictamen := '9';
    else
       OUT_FILE_NAME := 'RPT_NNNNN_'||FILE_NAME||'_'||i_periodo || '.csv';
       SP_WRITE_LOG(OUT_FILE_NAME);
       arr_dictamen := '0';
    end if;

    SP_WRITE_LOG('Exe SP_PROC_REPORT_DIARIO_RIESGOS');
    SP_PROC_REPORT_DIARIO_RIESGOS(COD_PERIODO_INI, COD_PERIODO_FIN, ID_FLUJO, arr_dictamen, OUT_ROL_ROWS, return_code);

    curid := DBMS_SQL.to_cursor_number (OUT_ROL_ROWS);
    DBMS_SQL.describe_columns (curid , colcnt, desctab);

    --SP_WRITE_LOG('colcnt :' || colcnt);
    FOR indx IN 1 .. colcnt
    LOOP
      BUFFER := BUFFER || desctab (indx).col_name   || CHR(59);
      IF desctab (indx).col_type = 2 THEN
         DBMS_SQL.define_column (curid , indx, numvar);
      ELSIF desctab (indx).col_type = 12 THEN
         DBMS_SQL.define_column (curid , indx, datevar);
      ELSE
         DBMS_SQL.define_column (curid , indx, namevar, 100);
      END IF;
    END LOOP;

   WHILE DBMS_SQL.fetch_rows (curid) > 0
   LOOP
   BEGIN
     if row_cnt = 0 then
       if FILE_NAME = 'ERROR' then
         UTL_FILE.fgetattr('DIR_MOTOR', OUT_FILE_NAME,err_file_exist,err_file_len,err_block_size);
         if err_file_exist then
           OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'A', 32767);
         else
           OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'W', 32767);
         end if;
       else
         OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'W', 32767);
       end if;

       --SP_WRITE_LOG('columnas :' || BUFFER);
       UTL_FILE.PUT_LINE(OUTPUT_FILE, BUFFER);
     end if;

     BUFFER := '';
     FOR indx IN 1 .. colcnt
      LOOP
         --SP_WRITE_LOG(desctab (indx).col_name || ' = ');
         IF (desctab (indx).col_type = 2)
         THEN
            DBMS_SQL.COLUMN_VALUE (curid, indx, numvar);
            BUFFER := BUFFER || numvar || CHR(59);
         ELSIF (desctab (indx).col_type = 12)
         THEN
            DBMS_SQL.COLUMN_VALUE (curid, indx, datevar);
            BUFFER := BUFFER || datevar || CHR(59);
         ELSE
            DBMS_SQL.COLUMN_VALUE (curid, indx, namevar);
            BUFFER := BUFFER || namevar || CHR(59);
         END IF;
      END LOOP;
      --SP_WRITE_LOG('fila :' || BUFFER);
      UTL_FILE.PUT_LINE(OUTPUT_FILE, BUFFER);
      row_cnt := row_cnt + 1;
   EXCEPTION
      WHEN OTHERS THEN
         SP_WRITE_LOG('SP_DL_REPORT_DIARIO_RIESGOS Ocurrio un error en buffer');
         SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
   END;
   END LOOP;

   if UTL_FILE.IS_OPEN(OUTPUT_FILE) then
     UTL_FILE.FFLUSH(OUTPUT_FILE);
   end if;
   DBMS_SQL.close_cursor (curid);

   SP_WRITE_LOG('Fin Loop ');

   if UTL_FILE.IS_OPEN(OUTPUT_FILE) then
     UTL_FILE.FCLOSE(OUTPUT_FILE);
   end if;
   /*if (row_cnt = 0)
   then
      --borrar el archivo dado que no tiene informacion
      SP_WRITE_LOG('borrar archivo '|| OUT_FILE_NAME);
      UTL_FILE.FREMOVE('DIR_MOTOR', OUT_FILE_NAME);
   end if;*/

   SP_WRITE_LOG('Fin Exportar TEXTO REPORT_DIARIO_RIESGOS');
   SP_WRITE_LOG('Fin SP_DL_REPORT_DIARIO_RIESGOS');

   O_RESULT := 1;
	 O_MENSAJE := 'ok';
EXCEPTION
    WHEN UTL_FILE.INVALID_PATH THEN
    SP_WRITE_LOG('SP_DL_REPORT_DIARIO_RIESGOS-UTL_FILE.INVALID_PATH: Ocurrio un error');
    SP_WRITE_LOG('ERROR: Ruta de archivo invalida.');
    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
    O_RESULT  := 99;
  	O_MENSAJE := SQLCODE||' ERROR '||SQLERRM;
    WHEN OTHERS THEN
    SP_WRITE_LOG('SP_DL_REPORT_DIARIO_RIESGOS-OTHERS: Ocurrio un error');
    SP_WRITE_LOG(SQLERRM);
    SP_WRITE_LOG(SQLCODE);
    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
    O_RESULT  := 99;
		O_MENSAJE := SQLCODE||' ERROR '||SQLERRM;
END SP_DL_REPORT_DIARIO_RIESGOS;

PROCEDURE SP_PROC_REPORT_DIARIO_RIESGOS(COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2, ID_FLUJO NUMBER, DICTAMEN VARCHAR2, P_RPT OUT SYS_REFCURSOR, return_code OUT NUMBER)
as
v_sql VARCHAR2(30000);
begin
  SP_WRITE_LOG('');
  SP_WRITE_LOG('Ini SP_PROC_REPORT_DIARIO_RIESGOS');
  return_code := 0;

  v_sql := 'select canal as CODIGO_CANAL,
            ID_EVALUACION,
            fecha_hora as FECHA_EVALUACION,
            tip_doc,
            nro_doc,
            cod_central,
            IN_FE_P_RUC as RUC_EMPRESA,
            CASE
            WHEN TRIM(IN_FE_P_FECHA_NACIMIENTO) IS NOT null THEN TRUNC((SYSDATE - TO_DATE(IN_FE_P_FECHA_NACIMIENTO, ''YYYY-MM-DD''))/ 365.25)
            END EDAD,
            ULT_RECHAZO,
            ULT_TAREA,
            ULT_RESULTADO,
            case IN_PC_P_NIVEL_RIESGO
            when ''LOW'' then ''RIESGO BAJO''
            when ''MEDIUM'' then ''RIESGO MEDIO''
            when ''MEDIUM_HIGH'' then ''RIESGO MEDIO ALTO''
            when ''HIGH'' then ''RIESGO ALTO''
            ELSE ''''
            END NIVEL_RIESGO,
            NVL(INGRESO_PH,''0'') AS INGRESO_PH,
            IN_FE_P_INGRESO_FIJO AS INGRESO_TABLON,
            CASE
            WHEN INGRESO_PH > IN_FE_P_INGRESO_FIJO THEN  INGRESO_PH
            WHEN IN_FE_P_INGRESO_FIJO >   NVL(INGRESO_PH,''0'') THEN  IN_FE_P_INGRESO_FIJO
            END INGRESO_FINAL,
            FLG_CAMPANA,
            LINEA_CAMPANA,
            IN_FE_P_CLASIFICACION_SBS as CLASIF_SF,
            IN_FE_P_CLASIFICACION_BBVA as CLASIF_BBVA,
            IN_FE_P_NUM_ENTIDADES_SALDO as NRO_ENTIDADES,
            IN_FE_P_CLIENTE_INDESEADO as COD_EXCLUSION,
            COD_EXCLUSION_EMPRESA,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFECSTG001'' then ''1''
            else ''0''
            END FLG_CASTIGO,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFECSIM001'' then ''1''
            else ''0''
            END FLG_IMPAGO,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEPRTE001'' then ''1''
            else ''0''
            END FLG_ENTIDADES,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEEFDR001'' then ''1''
            else ''0''
            END FLG_EDAD,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEMCFP001'' then ''1''
            else ''0''
            END FLG_FRAUDE,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEMCEM001'' then ''1''
            else ''0''
            END FLG_EMPLEADO,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEMCCI001'' then ''1''
            else ''0''
            END FLG_COD_EXCLUSION,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEINGD001'' then ''1''
            else ''0''
            END FLG_INGRESOS,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RMARREF001'' then ''1''
            else ''0''
            END FLG_REFINANCIAMIENTO,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RMARREF001'' then ''1''
            else ''0''
            END FLG_RIESGO_ALTO,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RRELPUB001'' then ''1''
            else ''0''
            END FLG_PEP,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RMARGAR001'' then ''1''
            else ''0''
            END FLG_GARANTIA,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RMARCES001'' then ''1''
            else ''0''
            END FLG_CESANTES,
            COD_RIESGO,
            IN_SB_P_BURO as BURO,
            IN_PA_P_SEGMENTO_BANCO as COD_SEGMENTO,
            IN_SA_P_RATIO_ENDEUDAMIENTO as FACTOR_ENDEUDAMIENTO,
            IN_CP_P_PPM as PPM_FINAL,
            PPM_DISPONIBLE,
            IN_PA_P_CUOTA_PROACTIVO as CUOTA_PROACTIVO,
            TASA_TC,
            PLAZO,
            RATIO_ENDEUDAMIENTO,
            MULTIPLICADOR_LINEA,
            OU_LI_P_LINEA_TC_MOTOR as LINEA_TC,
            DICTAMEN,
            CARGA_MOI,
            MOTIVO_FALLA_CARGA,
            CONTRATO,
            ESTADO_CONTRATO
            from (
              select di.id_evaluacion as id_evaluacion,
                   NULL as   INGRESO_PH,
                   '''' as FLG_CAMPANA,
                   '''' as LINEA_CAMPANA,
                   '''' as COD_EXCLUSION_EMPRESA,
                   '''' as COD_RIESGO,
                   '''' as PPM_DISPONIBLE,
                   '''' as TASA_TC,
                   '''' as PLAZO,
                   '''' as RATIO_ENDEUDAMIENTO,
                   '''' as MULTIPLICADOR_LINEA,
                   '''' as DICTAMEN,
                   '''' as CARGA_MOI,
                   '''' as MOTIVO_FALLA_CARGA,
                   '''' as CONTRATO,
                   '''' as ESTADO_CONTRATO,
                   to_char(rec.fe_usua_crea,''DD/MM/YYYY HH24:MI:SS AM'') as fecha_hora,
                   to_char(rec.fe_usua_crea,''DD/MM/YYYY'') as fecha,
                   to_char(rec.fe_usua_crea,''HH24:MI:SS AM'') as hora,
                   rec.session_id id_session,
                   cli.co_tipo_documento as tip_doc,
                   cli.de_documento as nro_doc,
                   cli.co_central as cod_central,
                   rec.co_tarea as ult_tarea,
                   rec.co_paso  as ult_paso,
                   rec.intentos as ult_intentos,
                   rec.de_rechazo as ult_rechazo,
                   rec.in_resultado as ult_resultado,
                   mcc.co_canal || ''-'' || mcc.de_canal as canal,
                   cev.co_subcanal as subcanal,
                   mcp.co_producto || ''-'' || mcp.de_producto as producto,
                   cev.co_subproducto as subproducto,
                   mdf.codigo_flujo,
                   di.param_value,
                   lpad(mdt.id_tarea,3,''0'')||lpad(mcf.id_funcion,3,''0'')||lpad(di.id_param,4,''0'') campo
            from moteva.mer_det_informacion di
            inner join moteva.mer_cat_cliente cli on di.id_cliente = cli.id_cliente
            inner join moteva.mer_cat_evaluacion cev on di.id_evaluacion = cev.id_evaluacion
            inner join moteva.mer_rel_eval_cliente rec on di.id_cliente = rec.id_cliente and di.id_evaluacion = rec.id_evaluacion
            left join moteva.mer_det_tarea mdt on di.codigo_tarea = mdt.co_tarea and mdt.id_flujo='||ID_FLUJO||'
            left join moteva.mer_cat_funcion mcf on di.codigo_funcion = mcf.co_funcion
            left join moteva.mer_cat_canal mcc on cev.co_canal = mcc.co_canal
            left join moteva.mer_cat_producto mcp on cev.co_producto = mcp.co_producto
            left join (
              select h1.id_flujo, h2.co_canal, h2.de_canal, h3.co_producto, h3.de_producto, h1.codigo_flujo, h4.co_canal co_subcanal, h5.co_producto co_subproducto
              from moteva.mer_det_flujo h1
              left join moteva.mer_cat_canal h2 on h1.id_canal=h2.id_canal
              left join moteva.mer_cat_canal h4 on h1.id_subcanal = h4.id_canal
              left join moteva.mer_cat_producto h3 on h1.id_producto = h3.id_producto
              left join moteva.mer_cat_producto h5 on h1.id_subproducto = h5.id_producto
            ) mdf on cev.co_canal = mdf.co_canal and cev.co_producto =mdf.co_producto
            and cev.co_subcanal = mdf.co_subcanal and (nvl(cev.co_subproducto,''xxxx'')=nvl(mdf.co_subproducto,''xxxx''))
            and mdf.id_flujo='||ID_FLUJO||'
            where mdf.id_flujo is not null and rec.IN_RESULTADO IN ('''||DICTAMEN||''') and di.codigo_funcion in
            (select f.co_funcion from moteva.mer_cat_funcion f where f.tipo_serv = ''PWC'')
            and (to_char(rec.fe_usua_crea,''YYYYMMDD'')>= '''||COD_PERIODO_INI|| '''
                and to_char(rec.fe_usua_crea,''YYYYMMDD'')<= '''||COD_PERIODO_FIN|| ''')
            )
             pivot ( min(param_value)
                    for (campo) in
                     (' || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION'))>0      then        FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX'))>0      then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS'))>0             then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS')             end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI'))>0              then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI')              end ||
                      '))
            order  by id_evaluacion' ;
  --SP_WRITE_LOG(v_sql);
  OPEN P_RPT FOR v_sql;
  SP_WRITE_LOG('Fin SP_PROC_REPORT_DIARIO_RIESGOS');
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      SP_WRITE_LOG('No se encontraron registros: ' || SQLERRM);
   WHEN OTHERS THEN
      SP_WRITE_LOG('Error de base de datos: ' || SQLERRM);
END SP_PROC_REPORT_DIARIO_RIESGOS;


PROCEDURE SP_PROC_REPORT_DIA_RIESGOS_2(COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2, ID_FLUJO NUMBER, DICTAMEN VARCHAR2, P_RPT OUT SYS_REFCURSOR, return_code OUT NUMBER)
as
v_sql VARCHAR2(30000);
begin
  SP_WRITE_LOG('');
  SP_WRITE_LOG('Ini SP_PROC_REPORT_DIARIO_RIESGOS');
  return_code := 0;

  v_sql := 'select canal as CODIGO_CANAL,
            ID_EVALUACION,
            fecha_hora as FECHA_EVALUACION,
            tip_doc,
            nro_doc,
            cod_central,
            IN_FE_P_RUC as RUC_EMPRESA,
            CASE
            WHEN TRIM(IN_FE_P_FECHA_NACIMIENTO) IS NOT null THEN TRUNC((SYSDATE - TO_DATE(IN_FE_P_FECHA_NACIMIENTO, ''YYYY-MM-DD''))/ 365.25)
            END EDAD,
            ULT_RECHAZO,
            ULT_TAREA,
            ULT_RESULTADO,
            case IN_PC_P_NIVEL_RIESGO
            when ''LOW'' then ''RIESGO BAJO''
            when ''MEDIUM'' then ''RIESGO MEDIO''
            when ''MEDIUM_HIGH'' then ''RIESGO MEDIO ALTO''
            when ''HIGH'' then ''RIESGO ALTO''
            ELSE ''''
            END NIVEL_RIESGO,
            NVL(INGRESO_PH,''0'') AS INGRESO_PH,
            IN_FE_P_INGRESO_FIJO AS INGRESO_TABLON,
            INGRESO_FINAL,
            FLG_CAMPANA,
            LINEA_CAMPANA,
            IN_FE_P_CLASIFICACION_SBS as CLASIF_SF,
            IN_FE_P_CLASIFICACION_BBVA as CLASIF_BBVA,
            IN_FE_P_NUM_ENTIDADES_SALDO as NRO_ENTIDADES,
            IN_FE_P_CLIENTE_INDESEADO as COD_EXCLUSION,
            COD_EXCLUSION_EMPRESA,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFECSTG001'' then ''1''
            else ''0''
            END FLG_CASTIGO,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFECSIM001'' then ''1''
            else ''0''
            END FLG_IMPAGO,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEPRTE001'' then ''1''
            else ''0''
            END FLG_ENTIDADES,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEEFDR001'' then ''1''
            else ''0''
            END FLG_EDAD,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEMCFP001'' then ''1''
            else ''0''
            END FLG_FRAUDE,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEMCEM001'' then ''1''
            else ''0''
            END FLG_EMPLEADO,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEMCCI001'' then ''1''
            else ''0''
            END FLG_COD_EXCLUSION,
            case ULT_RECHAZO
            when ''RULE_PWC_FE_RFEINGD001'' then ''1''
            else ''0''
            END FLG_INGRESOS,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RMARREF001'' then ''1''
            else ''0''
            END FLG_REFINANCIAMIENTO,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RMARREF001'' then ''1''
            else ''0''
            END FLG_RIESGO_ALTO,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RRELPUB001'' then ''1''
            else ''0''
            END FLG_PEP,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RMARGAR001'' then ''1''
            else ''0''
            END FLG_GARANTIA,
            case ULT_RECHAZO
            when ''VLDTN_PWC_PC_RMARCES001'' then ''1''
            else ''0''
            END FLG_CESANTES,
            COD_RIESGO,
            IN_SB_P_BURO as BURO,
            IN_PA_P_SEGMENTO_BANCO as COD_SEGMENTO,
            IN_SA_P_RATIO_ENDEUDAMIENTO as FACTOR_ENDEUDAMIENTO,
            IN_CP_P_PPM as PPM_FINAL,
            PPM_DISPONIBLE,
            IN_PA_P_CUOTA_PROACTIVO as CUOTA_PROACTIVO,
            TASA_TC,
            PLAZO,
            RATIO_ENDEUDAMIENTO,
            MULTIPLICADOR_LINEA,
            OU_LI_P_LINEA_TC_MOTOR as LINEA_TC,
            DICTAMEN,
            CARGA_MOI,
            MOTIVO_FALLA_CARGA,
            CONTRATO,
            ESTADO_CONTRATO
            from (
              select di.id_evaluacion as id_evaluacion,
                   NULL as   INGRESO_PH,
                   case
                    when ( dm.dictamen = ''NO EVALUADO'' or dm.carga_moi = ''EFECTIVO'' ) then ''1''
                    else ''0''
                   end FLG_CAMPANA,
                   case FLG_CAMPANA
                    when ''1'' then mdm.det_dictamen
                    else ''0''
                   end LINEA_CAMPANA,
                   NVL(ctl.cod_exclusion_empresa, '''') as COD_EXCLUSION_EMPRESA,
                   NVL(ctl.cliente_indeseado, '''') as COD_RIESGO,
                   '''' as PPM_DISPONIBLE,
                   '''' as TASA_TC,
                   '''' as PLAZO,
                   '''' as RATIO_ENDEUDAMIENTO,
                   '''' as MULTIPLICADOR_LINEA,
                   '''' as DICTAMEN,
                   dm.carga_moi as CARGA_MOI,
                   NVL(dm.motivo_falla_carga, '''') as MOTIVO_FALLA_CARGA,
                   erm.nro_contrato as CONTRATO,
                   erm.estado_contrato as ESTADO_CONTRATO,
                   to_char(rec.fe_usua_crea,''DD/MM/YYYY HH24:MI:SS AM'') as fecha_hora,
                   to_char(rec.fe_usua_crea,''DD/MM/YYYY'') as fecha,
                   to_char(rec.fe_usua_crea,''HH24:MI:SS AM'') as hora,
                   rec.session_id id_session,
                   cli.co_tipo_documento as tip_doc,
                   cli.de_documento as nro_doc,
                   cli.co_central as cod_central,
                   rec.co_tarea as ult_tarea,
                   rec.co_paso  as ult_paso,
                   rec.intentos as ult_intentos,
                   rec.de_rechazo as ult_rechazo,
                   rec.in_resultado as ult_resultado,
                   mcc.co_canal || ''-'' || mcc.de_canal as canal,
                   cev.co_subcanal as subcanal,
                   mcp.co_producto || ''-'' || mcp.de_producto as producto,
                   cev.co_subproducto as subproducto,
                   mdf.codigo_flujo,
                   di.param_value,
                   lpad(mdt.id_tarea,3,''0'')||lpad(mcf.id_funcion,3,''0'')||lpad(di.id_param,4,''0'') campo,
                   dm.ingreso_final as INGRESO_FINAL,
            from moteva.mer_det_informacion di
            inner join moteva.mer_cat_cliente cli on di.id_cliente = cli.id_cliente
            inner join moteva.mer_cat_evaluacion cev on di.id_evaluacion = cev.id_evaluacion
            inner join moteva.mer_rel_eval_cliente rec on di.id_cliente = rec.id_cliente and di.id_evaluacion = rec.id_evaluacion
            inner join moteva.mer_detalle_moi dm on di.id_evaluacion = dm.id_evaluacion
            inner join moteva.mer_cat_tb_light ctl on cli.de_documento = ctl.documento
            inner join moteva.mer_ext_reporte_moi erm on cli.de_documento = erm.nro_documento
            left join moteva.mer_det_tarea mdt on di.codigo_tarea = mdt.co_tarea and mdt.id_flujo='||ID_FLUJO||'
            left join moteva.mer_cat_funcion mcf on di.codigo_funcion = mcf.co_funcion
            left join moteva.mer_cat_canal mcc on cev.co_canal = mcc.co_canal
            left join moteva.mer_cat_producto mcp on cev.co_producto = mcp.co_producto
            left join (
              select h1.id_flujo, h2.co_canal, h2.de_canal, h3.co_producto, h3.de_producto, h1.codigo_flujo, h4.co_canal co_subcanal, h5.co_producto co_subproducto
              from moteva.mer_det_flujo h1
              left join moteva.mer_cat_canal h2 on h1.id_canal=h2.id_canal
              left join moteva.mer_cat_canal h4 on h1.id_subcanal = h4.id_canal
              left join moteva.mer_cat_producto h3 on h1.id_producto = h3.id_producto
              left join moteva.mer_cat_producto h5 on h1.id_subproducto = h5.id_producto
            ) mdf on cev.co_canal = mdf.co_canal and cev.co_producto =mdf.co_producto
            and cev.co_subcanal = mdf.co_subcanal and (nvl(cev.co_subproducto,''xxxx'')=nvl(mdf.co_subproducto,''xxxx''))
            and mdf.id_flujo='||ID_FLUJO||'
            where mdf.id_flujo is not null and rec.IN_RESULTADO IN ('''||DICTAMEN||''') and di.codigo_funcion in
            (select f.co_funcion from moteva.mer_cat_funcion f where f.tipo_serv = ''PWC'')
            and (to_char(rec.fe_usua_crea,''YYYYMMDD'')>= '''||COD_PERIODO_INI|| '''
                and to_char(rec.fe_usua_crea,''YYYYMMDD'')<= '''||COD_PERIODO_FIN|| ''')
            )
             pivot ( min(param_value)
                    for (campo) in
                     (' || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION'))>0      then        FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX'))>0      then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS'))>0             then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS')             end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI'))>0              then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI')              end ||
                      '))
            order  by id_evaluacion' ;
  --SP_WRITE_LOG(v_sql);
  OPEN P_RPT FOR v_sql;
  SP_WRITE_LOG('Fin SP_PROC_REPORT_DIARIO_RIESGOS');
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      SP_WRITE_LOG('No se encontraron registros: ' || SQLERRM);
   WHEN OTHERS THEN
      SP_WRITE_LOG('Error de base de datos: ' || SQLERRM);
END SP_PROC_REPORT_DIA_RIESGOS_2;


PROCEDURE SP_DL_REPORT_NOMINAS(COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2, ID_FLUJO NUMBER, FILE_NAME VARCHAR2, TIPO_RPT VARCHAR2, O_RESULT OUT INT, O_MENSAJE OUT VARCHAR2)
AS
    OUT_ROL_ROWS  SYS_REFCURSOR;
    OUT_FILE_NAME VARCHAR2(400):='';
    OUTPUT_FILE   UTL_FILE.FILE_TYPE;
    BUFFER        VARCHAR2(30000);
    STMT          VARCHAR2(200);
    STMH          VARCHAR2(200);
    FECHA_PROCESO VARCHAR(200);
    HORA_PROCESO  VARCHAR(200);
    ENCABEZADO    VARCHAR(3500);
    i_periodo     VARCHAR2(8);
    return_code   NUMBER;

    curid         INTEGER;
    colcnt        INTEGER:=0;
    desctab       dbms_sql.desc_tab;

    namevar    VARCHAR2 (100);
    numvar     NUMBER;
    datevar    DATE;
    row_cnt    INTEGER:=0;
    err_file_exist boolean:=false;
    err_file_len number;
    err_block_size number;

    arr_dictamen VARCHAR2(100):='';
BEGIN
    SP_WRITE_LOG('');
    SP_WRITE_LOG('Ini SP_DL_REPORT_NOMINAS');
    SP_WRITE_LOG('Exportar TEXTO REPORT_NOMINAS');
    i_periodo :=  COD_PERIODO_INI; --FORMATO YYYYMMDD
    STMT := 'SELECT TO_CHAR(SYSDATE, ' || CHR(39) ||
            'YYYYMMDD'      || CHR(39) ||
            ') FROM DUAL';
    STMH := 'SELECT TO_CHAR(SYSDATE, ' || CHR(39) ||
            'HH24MISS'      || CHR(39) ||
            ') FROM DUAL';

    EXECUTE IMMEDIATE (STMT) INTO FECHA_PROCESO;
    EXECUTE IMMEDIATE (STMH) INTO HORA_PROCESO;

    if TIPO_RPT = 'D' then
       if (to_date(COD_PERIODO_FIN, 'YYYYMMDD') - to_date(COD_PERIODO_INI, 'YYYYMMDD')) = 0 then
          OUT_FILE_NAME := 'RPT_NOMINA_'||FILE_NAME||'_'||i_periodo || '.csv';
       else
          OUT_FILE_NAME := 'RPT_NOMINA_'||FILE_NAME||'_'||i_periodo||'_'||COD_PERIODO_FIN || '.csv';
       end if;
       SP_WRITE_LOG(OUT_FILE_NAME);
       arr_dictamen := '1'',''2'',''3';
    elsif TIPO_RPT = 'M' then
       if (to_date(COD_PERIODO_FIN, 'YYYYMMDD') - to_date(COD_PERIODO_INI, 'YYYYMMDD')) = 0 then
          OUT_FILE_NAME := 'RPT_NOMINA_'||FILE_NAME||'_'||i_periodo || '.csv';
       else
          OUT_FILE_NAME := 'RPT_NOMINA_'||FILE_NAME||'_'||i_periodo||'_'||COD_PERIODO_FIN || '.csv';
       end if;
       SP_WRITE_LOG(OUT_FILE_NAME);
--       arr_dictamen := '9';
       arr_dictamen := '1'',''2'',''3';
    else
       OUT_FILE_NAME := 'RPT_NNNNN_'||FILE_NAME||'_'||i_periodo || '.csv';
       SP_WRITE_LOG(OUT_FILE_NAME);
       arr_dictamen := '0';
    end if;

    if TIPO_RPT = 'D' then
      SP_WRITE_LOG('Exe SP_PROC_REPORT_NOMINAS_DIARIO');
      SP_PROC_REPORT_NOMINAS(COD_PERIODO_INI, COD_PERIODO_FIN, ID_FLUJO, TIPO_RPT, arr_dictamen, OUT_ROL_ROWS, return_code);
    elsif TIPO_RPT = 'M' then
      SP_WRITE_LOG('Exe SP_PROC_REPORT_NOMINAS_MENSUAL');
      SP_PROC_REPORT_NOMINAS(COD_PERIODO_INI, COD_PERIODO_FIN, ID_FLUJO, TIPO_RPT, arr_dictamen, OUT_ROL_ROWS, return_code);
    end if;

    curid := DBMS_SQL.to_cursor_number (OUT_ROL_ROWS);
    DBMS_SQL.describe_columns (curid , colcnt, desctab);

    --SP_WRITE_LOG('colcnt :' || colcnt);
    FOR indx IN 1 .. colcnt
    LOOP
      BUFFER := BUFFER || desctab (indx).col_name   || CHR(59);
      IF desctab (indx).col_type = 2 THEN
         DBMS_SQL.define_column (curid , indx, numvar);
      ELSIF desctab (indx).col_type = 12 THEN
         DBMS_SQL.define_column (curid , indx, datevar);
      ELSE
         DBMS_SQL.define_column (curid , indx, namevar, 100);
      END IF;
    END LOOP;

   WHILE DBMS_SQL.fetch_rows (curid) > 0
   LOOP
   BEGIN
     if row_cnt = 0 then
       if FILE_NAME = 'ERROR' then
         UTL_FILE.fgetattr('DIR_MOTOR', OUT_FILE_NAME,err_file_exist,err_file_len,err_block_size);
         if err_file_exist then
           OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'A', 32767);
         else
           OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'W', 32767);
         end if;
       else
         OUTPUT_FILE   := UTL_FILE.FOPEN('DIR_MOTOR', OUT_FILE_NAME, 'W', 32767);
       end if;

       --SP_WRITE_LOG('columnas :' || BUFFER);
       UTL_FILE.PUT_LINE(OUTPUT_FILE, BUFFER);
     end if;

     BUFFER := '';
     FOR indx IN 1 .. colcnt
      LOOP
         --SP_WRITE_LOG(desctab (indx).col_name || ' = ');
         IF (desctab (indx).col_type = 2)
         THEN
            DBMS_SQL.COLUMN_VALUE (curid, indx, numvar);
            BUFFER := BUFFER || numvar || CHR(59);
         ELSIF (desctab (indx).col_type = 12)
         THEN
            DBMS_SQL.COLUMN_VALUE (curid, indx, datevar);
            BUFFER := BUFFER || datevar || CHR(59);
         ELSE
            DBMS_SQL.COLUMN_VALUE (curid, indx, namevar);
            BUFFER := BUFFER || namevar || CHR(59);
         END IF;
      END LOOP;
      --SP_WRITE_LOG('fila :' || BUFFER);
      UTL_FILE.PUT_LINE(OUTPUT_FILE, BUFFER);
      row_cnt := row_cnt + 1;
   EXCEPTION
      WHEN OTHERS THEN
         SP_WRITE_LOG('SP_DL_REPORT_NOMINAS Ocurrio un error en buffer');
         SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
   END;
   END LOOP;

   if UTL_FILE.IS_OPEN(OUTPUT_FILE) then
     UTL_FILE.FFLUSH(OUTPUT_FILE);
   end if;
   DBMS_SQL.close_cursor (curid);

   SP_WRITE_LOG('Fin Loop ');

   if UTL_FILE.IS_OPEN(OUTPUT_FILE) then
     UTL_FILE.FCLOSE(OUTPUT_FILE);
   end if;
   /*if (row_cnt = 0)
   then
      --borrar el archivo dado que no tiene informacion
      SP_WRITE_LOG('borrar archivo '|| OUT_FILE_NAME);
      UTL_FILE.FREMOVE('DIR_MOTOR', OUT_FILE_NAME);
   end if;*/

   SP_WRITE_LOG('Fin Exportar TEXTO REPORT_NOMINAS');
   SP_WRITE_LOG('Fin SP_DL_REPORT_NOMINAS');

   O_RESULT := 1;
	 O_MENSAJE := 'ok';
EXCEPTION
    WHEN UTL_FILE.INVALID_PATH THEN
    SP_WRITE_LOG('SP_DL_REPORT_NOMINAS-UTL_FILE.INVALID_PATH: Ocurrio un error');
    SP_WRITE_LOG('ERROR: Ruta de archivo invalida.');
    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
    O_RESULT  := 99;
  	O_MENSAJE := SQLCODE||' ERROR '||SQLERRM;
    WHEN OTHERS THEN
    SP_WRITE_LOG('SP_DL_REPORT_NOMINAS-OTHERS: Ocurrio un error');
    SP_WRITE_LOG(SQLERRM);
    SP_WRITE_LOG(SQLCODE);
    SP_WRITE_LOG(SQLCODE||' -ERROR- '||SQLERRM);
    O_RESULT  := 99;
		O_MENSAJE := SQLCODE||' ERROR '||SQLERRM;
END SP_DL_REPORT_NOMINAS;

PROCEDURE SP_PROC_REPORT_NOMINAS(COD_PERIODO_INI VARCHAR2, COD_PERIODO_FIN VARCHAR2, ID_FLUJO NUMBER, TIPO_RPT VARCHAR2, DICTAMEN VARCHAR2, P_RPT OUT SYS_REFCURSOR, return_code OUT NUMBER)
as
v_sql VARCHAR2(30000);
begin
  SP_WRITE_LOG('');
  SP_WRITE_LOG('Ini SP_PROC_REPORT_NOMINAS');
  return_code := 0;

  if TIPO_RPT = 'D' then
    v_sql := 'select
            tip_doc,
            nro_doc,
            IN_FE_P_FECHA_NACIMIENTO as FECHA_NACIMIENTO,
            INGRESO,
            IN_FE_P_RUC as RUC_EMPRESA,
            case dictamen
            when ''1'' then ''APROBADO''
            when ''2'' then ''RECHAZADO''
            when ''3'' then ''NO EVALUADO''
            ELSE ''''
            END DICTAMEN,
            DET_DICTAMEN,
            case CARGA_MOI
            when ''1'' then ''EFECTIVA''
            when ''2'' then ''NO CARGO''
            when ''9'' then ''FALLO''
            ELSE ''''
            END CARGA_MOI,
            MOTIVO_FALLA_CARGA,
            NUMERO_CONTRATO,
            PRODUCTO
            from (
              select di.id_evaluacion as id_evaluacion,
                   mdm.ingreso,
                   mdm.dictamen,
                   mdm.det_dictamen,
                   mdm.carga_moi,
                   mdm.motivo_falla_carga,
                   '''' as NUMERO_CONTRATO,
                   ''TC'' as PRODUCTO,
                   cli.co_tipo_documento as tip_doc,
                   cli.de_documento as nro_doc,
                   di.param_value,
                   lpad(mdt.id_tarea,3,''0'')||lpad(mcf.id_funcion,3,''0'')||lpad(di.id_param,4,''0'') campo
            from moteva.mer_det_informacion di
            inner join moteva.mer_cat_cliente cli on di.id_cliente = cli.id_cliente
            inner join moteva.mer_cat_evaluacion cev on di.id_evaluacion = cev.id_evaluacion
            inner join moteva.mer_rel_eval_cliente rec on di.id_cliente = rec.id_cliente and di.id_evaluacion = rec.id_evaluacion
            inner join moteva.mer_detalle_moi mdm on di.id_evaluacion = mdm.id_evaluacion
            left join moteva.mer_det_tarea mdt on di.codigo_tarea = mdt.co_tarea and mdt.id_flujo='||ID_FLUJO||'
            left join moteva.mer_cat_funcion mcf on di.codigo_funcion = mcf.co_funcion
            left join moteva.mer_cat_canal mcc on cev.co_canal = mcc.co_canal
            left join moteva.mer_cat_producto mcp on cev.co_producto = mcp.co_producto
            left join (
              select h1.id_flujo, h2.co_canal, h2.de_canal, h3.co_producto, h3.de_producto, h1.codigo_flujo, h4.co_canal co_subcanal, h5.co_producto co_subproducto
              from moteva.mer_det_flujo h1
              left join moteva.mer_cat_canal h2 on h1.id_canal=h2.id_canal
              left join moteva.mer_cat_canal h4 on h1.id_subcanal = h4.id_canal
              left join moteva.mer_cat_producto h3 on h1.id_producto = h3.id_producto
              left join moteva.mer_cat_producto h5 on h1.id_subproducto = h5.id_producto
            ) mdf on cev.co_canal = mdf.co_canal and cev.co_producto =mdf.co_producto
            and cev.co_subcanal = mdf.co_subcanal and (nvl(cev.co_subproducto,''xxxx'')=nvl(mdf.co_subproducto,''xxxx''))
            and mdf.id_flujo='||ID_FLUJO||'
            where mdf.id_flujo is not null and rec.IN_RESULTADO IN ('''||DICTAMEN||''') and di.codigo_funcion in
            (select f.co_funcion from moteva.mer_cat_funcion f where f.tipo_serv = ''PWC'')
            and (to_char(rec.fe_usua_crea,''YYYYMMDD'')>= '''||COD_PERIODO_INI|| '''
                and to_char(rec.fe_usua_crea,''YYYYMMDD'')<= '''||COD_PERIODO_FIN|| ''')
            )
             pivot ( min(param_value)
                    for (campo) in
                     (' || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION'))>0      then        FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX'))>0      then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS'))>0             then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS')             end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI'))>0              then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI')              end ||
                      '))
            order  by id_evaluacion' ;
       --     SP_WRITE_LOG('QUERY DIARIO:'||v_sql);
  elsif TIPO_RPT = 'M' then
    v_sql := 'select
            ID_EVALUACION,
            tip_doc,
            nro_doc,
            IN_FE_P_FECHA_NACIMIENTO as FECHA_NACIMIENTO,
            INGRESO,
            IN_FE_P_RUC as RUC_EMPRESA,
            case dictamen
            when ''1'' then ''APROBADO''
            when ''2'' then ''RECHAZADO''
            when ''3'' then ''NO EVALUADO''
            ELSE ''''
            END DICTAMEN,
            DET_DICTAMEN,
            case CARGA_MOI
            when ''1'' then ''EFECTIVA''
            when ''2'' then ''NO CARGO''
            when ''9'' then ''FALLO''
            ELSE ''''
            END CARGA_MOI,
            MOTIVO_FALLA_CARGA,
            NUMERO_CONTRATO,
            PRODUCTO
            from (
              select di.id_evaluacion as id_evaluacion,
                   mdm.ingreso,
                   mdm.dictamen,
                   mdm.det_dictamen,
                   mdm.carga_moi,
                   mdm.motivo_falla_carga,
                   mer.nro_contrato as NUMERO_CONTRATO,
                   ''TC'' as PRODUCTO,
                   cli.co_tipo_documento as tip_doc,
                   cli.de_documento as nro_doc,
                   di.param_value,
                   lpad(mdt.id_tarea,3,''0'')||lpad(mcf.id_funcion,3,''0'')||lpad(di.id_param,4,''0'') campo
            from moteva.mer_det_informacion di
            inner join moteva.mer_cat_cliente cli on di.id_cliente = cli.id_cliente
            inner join moteva.mer_cat_evaluacion cev on di.id_evaluacion = cev.id_evaluacion
            inner join moteva.mer_rel_eval_cliente rec on di.id_cliente = rec.id_cliente and di.id_evaluacion = rec.id_evaluacion
            inner join moteva.mer_detalle_moi mdm on di.id_evaluacion = mdm.id_evaluacion
            left join moteva.mer_ext_reporte_moi mer on cli.co_tipo_documento = mer.tipo_documento and  cli.de_documento = mer.nro_documento
            left join moteva.mer_det_tarea mdt on di.codigo_tarea = mdt.co_tarea and mdt.id_flujo='||ID_FLUJO||'
            left join moteva.mer_cat_funcion mcf on di.codigo_funcion = mcf.co_funcion
            left join moteva.mer_cat_canal mcc on cev.co_canal = mcc.co_canal
            left join moteva.mer_cat_producto mcp on cev.co_producto = mcp.co_producto
            left join (
              select h1.id_flujo, h2.co_canal, h2.de_canal, h3.co_producto, h3.de_producto, h1.codigo_flujo, h4.co_canal co_subcanal, h5.co_producto co_subproducto
              from moteva.mer_det_flujo h1
              left join moteva.mer_cat_canal h2 on h1.id_canal=h2.id_canal
              left join moteva.mer_cat_canal h4 on h1.id_subcanal = h4.id_canal
              left join moteva.mer_cat_producto h3 on h1.id_producto = h3.id_producto
              left join moteva.mer_cat_producto h5 on h1.id_subproducto = h5.id_producto
            ) mdf on cev.co_canal = mdf.co_canal and cev.co_producto =mdf.co_producto
            and cev.co_subcanal = mdf.co_subcanal and (nvl(cev.co_subproducto,''xxxx'')=nvl(mdf.co_subproducto,''xxxx''))
            and mdf.id_flujo='||ID_FLUJO||'
            where mdf.id_flujo is not null and rec.IN_RESULTADO IN ('''||DICTAMEN||''') and di.codigo_funcion in
            (select f.co_funcion from moteva.mer_cat_funcion f where f.tipo_serv = ''PWC'')
            and (to_char(rec.fe_usua_crea,''YYYYMMDD'')>= '''||COD_PERIODO_INI|| '''
                and to_char(rec.fe_usua_crea,''YYYYMMDD'')<= '''||COD_PERIODO_FIN|| ''')
            )
             pivot ( min(param_value)
                    for (campo) in
                     (' || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION'))>0      then        FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_FILTROS_EXCLUSION')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_VALIDACIONES_INTERNAS')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_ESTRATEGIAS_PRELLAMADA') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_BURO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX'))>0      then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SCREENING_EQUIFAX')      end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION'))>0 then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CLUSTERING_PREDECISION') end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO'))>0         then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CAPACIDAD_PAGO')         end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA'))>0  then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_EVALUACION_CREDITICIA')  end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_LIMITES')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS'))>0             then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_PARAMETROS')             end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION'))>0                then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_SANCION')                end
                        || case when LENGTH(FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI'))>0              then ',' || FN_GET_ROW_TO_HEADER(ID_FLUJO,'T_CARGA_MOI')              end ||
                      '))
            order  by id_evaluacion' ;
        --    SP_WRITE_LOG('QUERY MENSUAL:'||v_sql);
  end if;
  --SP_WRITE_LOG(v_sql);
  OPEN P_RPT FOR v_sql;
  SP_WRITE_LOG('Fin SP_PROC_REPORT_NOMINAS');
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      SP_WRITE_LOG('No se encontraron registros: ' || SQLERRM);
   WHEN OTHERS THEN
      SP_WRITE_LOG('Error de base de datos: ' || SQLERRM);
END SP_PROC_REPORT_NOMINAS;
END PPYM_REPORT;
