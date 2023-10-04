###########################################################
#Autor: Violeta Gracia
#Fecha: 09/2023
###########################################################


###########################################################


import tensorflow as tf
import numpy as np
def create_input(img_path,potencia_instalada,img_size=(128, 128)):
    img=tf.keras.preprocessing.image.load_img(img_path, target_size=img_size)
    img=tf.keras.preprocessing.image.img_to_array(img)/255.
    img = np.expand_dims(img, axis=0)
    return (img, np.array([[potencia_instalada]]))