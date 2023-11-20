import pandas as pd

def asigna_aparecio_muerto(df : pd ):
# Crea un diccionario con los reemplazos
    reemplazos = {
        "009509":{"situacion_actual_des":"Apareció Muerto", "documento":"91092274301"},
        "137587":{"situacion_actual_des":"Apareció Muerto", "documento":"1060207279"},
        "009063":{"situacion_actual_des":"Apareció Muerto", "documento":"820445"},
        "008416":{"situacion_actual_des":"Apareció Muerto", "documento":"4572934"},
        "125357":{"situacion_actual_des":"Apareció Muerto", "documento":"903939"},
        "007720":{"situacion_actual_des":"Apareció Muerto", "documento":"9040709"},
        "123524":{"situacion_actual_des":"Apareció Muerto", "documento":"2971289"},
        "000585":{"situacion_actual_des":"Apareció Muerto", "documento":"20294379"},
        "163619":{"situacion_actual_des":"Apareció Muerto", "documento":"214573"},
        "004743":{"situacion_actual_des":"Apareció Muerto", "documento":"96187763"},
        "006060":{"situacion_actual_des":"Apareció Muerto", "documento":"84082262287"},
        "004562":{"situacion_actual_des":"Apareció Muerto", "documento":"97610652"},
        "003780":{"situacion_actual_des":"Apareció Muerto", "documento":"9863273"},
        "004142":{"situacion_actual_des":"Apareció Muerto", "documento":"1002954986"},
        "001781":{"situacion_actual_des":"Apareció Muerto", "documento":"88111584340"},
        "009069":{"situacion_actual_des":"Apareció Muerto", "documento":"2097502"},
        "008284":{"situacion_actual_des":"Apareció Muerto", "documento":"20687044"},
        "003922":{"situacion_actual_des":"Apareció Muerto", "documento":"1978959"},
        "003994":{"situacion_actual_des":"Apareció Muerto", "documento":"86083931"},       
        "005802":{"situacion_actual_des":"Apareció Muerto", "documento":"82011357861"},
        "143426":{"situacion_actual_des":"Apareció Muerto", "documento":"15675874"},
        "137105":{"situacion_actual_des":"Apareció Muerto", "documento":"92061263146"},
        "009516":{"situacion_actual_des":"Apareció Muerto", "documento":"94479477"},
        "003217":{"situacion_actual_des":"Apareció Muerto", "documento":"17783969"},
        "002896":{"situacion_actual_des":"Apareció Muerto", "documento":"11705762"},
        "000883":{"situacion_actual_des":"Apareció Muerto", "documento":"84013051673"},
        "005965":{"situacion_actual_des":"Apareció Muerto", "documento":"8375717"},
        "004768":{"situacion_actual_des":"Apareció Muerto", "documento":"93461326"},
        "125013":{"situacion_actual_des":"Apareció Muerto", "documento":"81032609597"},          
        "009605":{"situacion_actual_des":"Apareció Muerto", "documento":"9978630"},
        "002003":{"situacion_actual_des":"Apareció Muerto", "documento":"86042370411"},
        "000122":{"situacion_actual_des":"Apareció Muerto", "documento":"250219"},
        "000122":{"situacion_actual_des":"Apareció Muerto", "documento":"80159443"},
        "002204":{"situacion_actual_des":"Apareció Muerto", "documento":"1120565185"}, 
        "002616":{"situacion_actual_des":"Apareció Muerto", "documento":"92051066440"},
        "002388":{"situacion_actual_des":"Apareció Muerto", "documento":"5030713"},
        "002388":{"situacion_actual_des":"Apareció Muerto", "documento":"93110919458"}
    }
    
    # Itera sobre el diccionario y realiza los reemplazos en el DataFrame
    for codigo, datos_reemplazo in reemplazos.items():
        df.loc[df['codigo_unico_fuente'] == codigo, ['situacion_actual_des', 'documento']] = datos_reemplazo.values()
    
    reemplazos = {}
    reemplazos = {
        "005580":{"situacion_actual_des":"Apareció Muerto"},
        "161649":{"situacion_actual_des":"Apareció Muerto"},
        "002147":{"situacion_actual_des":"Apareció Muerto"},
        "005377":{"situacion_actual_des":"Apareció Muerto"},
        "000160":{"situacion_actual_des":"Apareció Muerto"},
        "005476":{"situacion_actual_des":"Apareció Muerto"},
        "009877":{"situacion_actual_des":"Apareció Muerto"},
        "001758":{"situacion_actual_des":"Apareció Muerto"}
    }
        
    for codigo, datos_reemplazo in reemplazos.items():
        df.loc[df['codigo_unico_fuente'] == codigo, 'situacion_actual_des'] = datos_reemplazo["situacion_actual_des"]
        
    reemplazos = {}
    reemplazos = {
        "163619":{"documento":"1123087248"}
    }
        
    for codigo, datos_reemplazo in reemplazos.items():
        df.loc[df['codigo_unico_fuente'] == codigo, 'documento'] = datos_reemplazo["documento"]