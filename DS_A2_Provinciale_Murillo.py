"""
Algoritmo para gestionar la exclusión mutua de un recurso
utilizando la libreria pywren.

Fecha:
    25/05/2020
Autores: 
        Marc Provinciale Isach
        Aaron Murillo Lort
    * Ambos autores han colaborado de forma equitativa y han implementado
      entre los dos todo el código del algoritmo. 
"""

import pywren_ibm_cloud as pywren
import json as j
import time

N_SLAVES = 100
BUCKET = 'marc-provinciale-bucket'
refresh = 0.01

# Función coordinadora que se encarga de dar permisos a las funciones esclavas
# para la compartición del fichero results.json 
def master(id, x, ibm_cos):
    i = 0
    write_permission_list = []
    ibm_cos.put_object(Bucket = BUCKET, Key='result.json',Body=j.dumps(write_permission_list))

    while i < N_SLAVES:

        # 1. monitor COS bucket each X seconds
        time.sleep(x)

        try:
            # 2. List all "p_write_{id}" files
            objects = ibm_cos.list_objects(Bucket = BUCKET, Prefix= 'p_write_')['Contents']
            # 3. Order objects by time of creation
            objects = sorted(objects, key = lambda k: k['LastModified'])

            # 4. Pop first object of the list "p_write_{id}"
            p_slave = objects.pop(0)['Key'][2:]
            
            # 5. Write empty "write_{id}" object into COS
            anterior = j.loads(ibm_cos.get_object(Bucket = BUCKET, Key= 'result.json')['Body'].read())
            ibm_cos.put_object(Bucket=BUCKET, Key=p_slave, Body=b'')
            write_permission_list.append(int(p_slave[6:]))

            # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
            ibm_cos.delete_object(Bucket=BUCKET, Key='p_'+p_slave)

            # 7. Monitor "result.txt" object each X seconds until it is updated
            actual = anterior
            while anterior == actual:
                time.sleep(x)
                actual = j.loads(ibm_cos.get_object(Bucket = BUCKET, Key= 'result.json')['Body'].read())

            # 8. Delete from COS “write_{id}”
            ibm_cos.delete_object(Bucket=BUCKET, Key=p_slave)

            # 9. Back to step 1 until no "p_write_{id}" objects in the bucket
            i = i + 1
        except:
            x = x
    
    return write_permission_list

# Función esclava que se encargará de modificar el fichero de result.json.
# Al tratarse de un recurso compartido solo podrá ser accedido cuando la función
# coordinadora le de permiso a la función esclava. 
def slave(id, x, ibm_cos):
    permiso = False

    # 1. Write empty "p_write_{id}" object into COS
    ibm_cos.put_object(Bucket= BUCKET, Key = 'p_write_'+str(id), Body='')
    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    while(permiso == False):
        time.sleep(x)
        try:
            ibm_cos.get_object(Bucket=BUCKET, Key = 'write_'+str(id))
            permiso = True
        except: 
            permiso = False
    
    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    
    write_permission_list = j.loads(ibm_cos.get_object(Bucket=BUCKET, Key='result.json')['Body'].read())
    write_permission_list.append(id)
    ibm_cos.put_object(Bucket=BUCKET, Key= 'result.json', Body=j.dumps(write_permission_list))

    # 4. Finish

# Función main que se encarga de inicializar el entorno pywren para la ejecución en la nube e
# inicializa las funciones esclavas y la coordinadora. Al final comprobará que el resultado de la
# función coordinadora sea igual al resultado del fichero result.json
if __name__ == '__main__':
    if(N_SLAVES > 100):
        N_SLAVES = 100
    if(N_SLAVES < 1):
        N_SLAVES = 1
    n = 0

    pw = pywren.ibm_cf_executor()
    ibm_cos = pw.internal_storage.get_client()
    
    slaves = []
    for i in range (0, N_SLAVES):
        slaves.append(refresh)
    pw.map(slave, slaves)
    pw.call_async(master, refresh)

    # Get result.txt
    pw.wait()
    txt = j.loads(ibm_cos.get_object(Bucket=BUCKET, Key='result.json')['Body'].read())
    result = pw.get_result()
    print('Fichero:')
    print(txt)
    print('\nMaster:')
    print(result)
    # check if content of result.txt == write_permission_list
    if txt == result:
        print('La exclusión se ha realizado correctamente.\n\n')
    else: print('Hay errores en la exclusión.\n\n')

    pw.clean()

 