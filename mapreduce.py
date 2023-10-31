# Codigo hecho por Ayrton Aranibar Castillo
import threading
import json

def eliminar_caracteres(texto, caracteres_a_eliminar):
    for caracter in caracteres_a_eliminar:
        texto = texto.replace(caracter, "")
    return texto

# Abre el archivo y realiza la limpieza
with open('DonQuijoteCap1.txt', 'r', encoding='utf-8') as archivo:
    contenido = archivo.read()

# Limpiamos el texto eliminando los siguientes caracteres
caracteres_a_eliminar = [",", ".", "(", ")", ";", "!", "¡"]
contenido = eliminar_caracteres(contenido, caracteres_a_eliminar)
contenido = contenido.lower() # todo a minúsculas

# Guardamos el archivo para comprobar al final
with open('DonQuijoteLimpio.txt', 'w', encoding='utf-8') as archivo_salida:
    archivo_salida.write(contenido)


def mapeo(archivo, mapa):
    archivo = archivo.split()
    for palabra in archivo:
        if palabra in mapa:
            mapa[palabra].append(1)
        else:
            mapa[palabra] = [1]
# Sincroniza los hilos para que no hayan interferencias y potr lo tanto herrores
lock = threading.Lock()

# Diccionario porque nos permite acceder de forma más directa y eficiente.
mapeo1 = {}
mapeo2 = {}
mapeo3 = {}

# Crear hilos para procesar el contenido
hilo1 = threading.Thread(target=mapeo, args=(contenido[:len(contenido)//3], mapeo1))
hilo2 = threading.Thread(target=mapeo, args=(contenido[len(contenido)//3:2*len(contenido)//3], mapeo2))
hilo3 = threading.Thread(target=mapeo, args=(contenido[2*len(contenido)//3:], mapeo3))

# Iniciar los hilos
hilo1.start()
hilo2.start()
hilo3.start()

# Esperar a que los hilos terminen
hilo1.join()
hilo2.join()
hilo3.join()

#print(mapeo1)
#print("/////////////////////////////////////////////////")
#print(mapeo2)
#print("/////////////////////////////////////////////////")
#print(mapeo3)


# Aqui iria el ordenamiento pero en este caso no es necesario y perderia rendimiento
# por la propia naturaleza del diccionario

mapa_global = [mapeo1,mapeo2,mapeo3]

# Primero junta todas las claves con sus valores, y de alli los separa en 2 para hacer reduce
def baraja(mapa_global):
    nuevo_mapa = {}  

    for mapa in mapa_global:
        for palabra, valores in mapa.items():
            if palabra in nuevo_mapa:
                nuevo_mapa[palabra].extend(valores)  # Extender la lista de valores existente
            else:
                nuevo_mapa[palabra] = valores  # Agregar la nueva palabra al nuevo mapa
    # Obtener las claves del nuevo mapa (nuevo_mapa)
    claves = list(nuevo_mapa.keys())

    # Calcular el punto medio para dividir
    punto_medio = len(claves) // 2

    # Dividir las claves en dos conjuntos
    conjunto1 = claves[:punto_medio]
    conjunto2 = claves[punto_medio:]

    # Crear dos nuevos mapas utilizando los conjuntos de claves
    nuevo_mapa1 = {clave: nuevo_mapa[clave] for clave in conjunto1}
    nuevo_mapa2 = {clave: nuevo_mapa[clave] for clave in conjunto2}
    return nuevo_mapa1,nuevo_mapa2

mapa1, mapa2 = baraja(mapa_global)
#print(mapa1)

def reduce(mapa, resultados):
    nuevo_mapa = {}
    for palabra, valores in mapa.items():
        suma_valores = 0
        for valor in valores:
            suma_valores += valor
        nuevo_mapa[palabra] = suma_valores
    resultados[threading.current_thread().name] = nuevo_mapa



# Crear una lista para almacenar los resultados
resultados =  {}

# Crear instancias de hilos para realizar la reducción en paralelo
hilo1 = threading.Thread(target=reduce, args=(mapa1, resultados))
hilo2 = threading.Thread(target=reduce, args=(mapa2, resultados))
# Iniciar los hilos
hilo1.start()
hilo2.start()

# Esperar a que los hilos terminen
hilo1.join()
hilo2.join()

resultado_total = {}

# Iterar a través de los hilos y combinar sus resultados en "resultados_combinados".
for thread_name, thread_result in resultados.items():
    resultado_total.update(thread_result)

# Ahora, "resultados_combinados" contendrá todos los elementos de los hilos combinados en un solo diccionario.
print(resultado_total)

# Guardo el documento con un formato de diccionario para aprovechar hash
cadena_diccionario = '{\n'
for clave, valor in resultado_total.items():
    cadena_diccionario += f"    '{clave}': {valor},\n"
cadena_diccionario += '}'

# Almacenamos el resultado en un documento 
with open('diccionario_conteo.txt', 'w') as archivo:
    archivo.write(cadena_diccionario)
