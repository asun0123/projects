#aircraft damage image classification model using ResNet, VGG, BLIP
#will display as 'dent' or 'crack'

import warnings
warnings.filterwarnings("ignore")
import os
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import zipfile
import keras
from keras.models import Sequential, Model
from keras.layers import Dense, Dropout, Flatten
from keras.applications import VGG16
from keras.optimizers import Adam
import tensorflow as tf
from tensorflow.keras.preprocessing.image import ImageDataGenerator
import matplotlib.pyplot as plt
import numpy as np
from keras.preprocessing import image
import random
import tarfile
import urllib.request
import os
import shutil
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.keras.preprocessing import image
import torch
import tensorflow as tf
from PIL import Image
from transformers import BlipProcessor, BlipForConditionalGeneration

#set seed value for reproducibility
seed_value = 42
random.seed(seed_value)
np.random.seed(seed_value)
tf.random.set_seed(seed_value)

#1. data preparation
batch_size = 32
n_epochs = 5
img_rows, img_cols = 224, 224
input_shape = (img_rows, img_cols, 3)

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/ZjXM4RKxlBK9_ZjHBLl5A/aircraft-damage-dataset-v1.tar"
tar_filename = "aircraft-damage-dataset-v1.tar"
extracted_folder = "aircraft-damage-dataset-v1"
urllib.request.urlretrieve(url, tar_filename)
print(f"Downloaded {tar_filename}, begin extraction")

if os.path.exists(extracted_folder):
    print(f"Folder {extracted_folder} already exists. Removing existing folder.")
    shutil.rmtree(extracted_folder)
    print(f"Removed existing folder {extracted_folder}.")

with tarfile.open(tar_filename, "r") as tar_ref:
    tar_ref.extractall()
    print(f"Extracted {tar_filename} to {extracted_folder}")

extract_path = "aircraft_damage_dataset-v1"
train_dir = os.path.join(extract_path, "train")
test_dir = os.path.join(extract_path, "test")
valid_dir = os.path.join(extract_path, "validation")

#2. data preprocessing
train_datagen = ImageDataGenerator(rescale=1./255)
valid_datagen = ImageDataGenerator(rescale=1./255)
test_datagen = ImageDataGenerator(rescale=1./255)

train_generator = train_datagen.flow_from_directory(
    train_dir,
    target_size=(img_rows, img_cols),   # Resize images to the size VGG16 expects
    batch_size=batch_size,
    seed = seed_value,
    class_mode='binary',
    shuffle=True
)

#Create valid generator and test generator
valid_generator = valid_datagen.flow_from_directory(
    directory=valid_dir,
    class_mode='binary',
    seed=seed_value,
    batch_size=batch_size,
    shuffle=False,
    target_size=(img_rows, img_cols)
)

test_generator = test_datagen.flow_from_directory(
    directory = test_dir,
    class_mode = 'binary',
    seed = seed_value,
    batch_size = batch_size,
    shuffle = False,
    target_size = (img_rows, img_cols)
)

#3. model building using VGG16
base_model = VGG16(
    weights='imagenet',
    include_top=False,
    input_shape=(img_rows, img_cols, 3)
)

output = base_model.layers[-1].output
output = keras.layers.Flatten()(output)
base_model = Model(base_model.input, output)

for layer in base_model.layers:
    layer.trainable = False

#custom model
model = Sequential()
model.add(base_model)
model.add(Dense(512, activation='relu'))
model.add(Dropout(0.3))
model.add(Dense(512, activation='relu'))
model.add(Dropout(0.3))
model.add(Dense(1, activation='sigmoid'))

model.compile(
    optimizer=Adam(learning_rate=0.0001),
    loss='binary_crossentropy',
    metrics=['accuracy']
)

history = model.fit(
    train_generator,
    epochs=n_epochs,
    validation_data=valid_generator
)

train_history = model.history.history

#4. vizualization
plt.title("Training Loss")
plt.ylabel("Loss")
plt.xlabel('Epoch')
plt.plot(train_history['loss'])
plt.show()

plt.title("Validation Loss")
plt.ylabel("Loss")
plt.xlabel('Epoch')
plt.plot(train_history['val_loss'])
plt.show()

#5. Training results evaluation
plt.title("Training Loss")
plt.ylabel("Loss")
plt.xlabel('Epoch')
plt.plot(train_history['loss'])
plt.show()

plt.title("Validation Loss")
plt.ylabel("Loss")
plt.xlabel('Epoch')
plt.plot(train_history['val_loss'])
plt.show()

#6. Model eval
test_loss, test_accuracy = model.evaluate(test_generator, steps=test_generator.samples // test_generator.batch_size)

print(f"Test Loss: {test_loss:.4f}")
print(f"Test Accuracy: {test_accuracy:.4f}")

def plot_image_with_title(image, model, true_label, predicted_label, class_names):
    plt.figure(figsize=(6, 6))
    plt.imshow(image)

    true_label_name = class_names[true_label]
    pred_label_name = class_names[predicted_label]

    plt.title(f"True: {true_label_name}\nPred: {pred_label_name}")
    plt.axis('off')
    plt.show()

# Function to test the model with images from the test set
def test_model_on_image(test_generator, model, index_to_plot=0):
    test_images, test_labels = next(test_generator)

    predictions = model.predict(test_images)

    predicted_classes = (predictions > 0.5).astype(int).flatten()

    class_indices = test_generator.class_indices
    class_names = {v: k for k, v in class_indices.items()}

    image_to_plot = test_images[index_to_plot]
    true_label = test_labels[index_to_plot]
    predicted_label = predicted_classes[index_to_plot]

    plot_image_with_title(image=image_to_plot, model=model, true_label=true_label, predicted_label=predicted_label, class_names=class_names)

index_to_plot = 1

test_model_on_image(
    test_generator,   # test_data_generator
    model,            # trained model
    index_to_plot=index_to_plot
)

#Image captioning and summarization using BLIP model
processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")

class BlipCaptionSummaryLayer(tf.keras.layers.Layer):
    def __init__(self, processor, model, **kwargs):
        super().__init__(**kwargs)
        self.processor = processor
        self.model = model

    def call(self, image_path, task):
        # Use tf.py_function to run the custom image processing and text generation
        return tf.py_function(self.process_image, [image_path, task], tf.string)

    def process_image(self, image_path, task):
        try:
            image_path_str = image_path.numpy().decode("utf-8")

            image = Image.open(image_path_str).convert("RGB")

            if task.numpy().decode("utf-8") == "caption":
                prompt = "This is a picture of" 
            else:
                prompt = "This is a detailed photo showing"

            inputs = self.processor(images=image, text=prompt, return_tensors="pt")

            output = self.model.generate(**inputs)

            result = self.processor.decode(output[0], skip_special_tokens=True)
            return result
        except Exception as e:
            print(f"Error: {e}")
            return "Error processing image"
        
def generate_text(image_path, task):
    blip_layer = BlipCaptionSummaryLayer(processor, model)
    return blip_layer([image_path, task])

image_path = tf.constant("aircraft_damage_dataset_v1/test/dent/144_10_JPG_jpg.rf.4d008cc33e217c1606b76585469d626b.jpg")  # actual path of image

#generate caption and summary
caption = generate_text(image_path, tf.constant("caption"))
print("Caption:", caption.numpy().decode("utf-8"))

summary = generate_text(image_path, tf.constant("summary"))
print("Summary:", summary.numpy().decode("utf-8"))

#Generate caption with BLIP
image_url = "aircraft_damage_dataset_v1/test/dent/149_22_JPG_jpg.rf.4899cbb6f4aad9588fa3811bb886c34d.jpg"
img = plt.imread(image_url)
plt.imshow(img)
plt.axis('off')
plt.show()

image_path = tf.constant("aircraft_damage_dataset_v1/test/dent/149_22_JPG_jpg.rf.4899cbb6f4aad9588fa3811bb886c34d.jpg")

image_path = "/content/sample_image.jpg"   # <-- Replace only if instructed

image_tensor = tf.constant(image_path)
task_tensor = tf.constant("caption")

caption_output = generate_text(image_tensor, task_tensor)

caption_output

#generate summary with BLIP
image_path = tf.constant("aircraft_damage_dataset_v1/test/dent/149_22_JPG_jpg.rf.4899cbb6f4aad9588fa3811bb886c34d.jpg")

image_path = "/content/sample_image.jpg"

image_tensor = tf.constant(image_path)
task_tensor = tf.constant("summary")

summary_output = generate_text(image_tensor, task_tensor)

summary_output

