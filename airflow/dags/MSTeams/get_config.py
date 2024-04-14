import jpype
import jpype.imports
from jpype import java
import os
import json

def get_config(jar_file_path, class_path='config/*', dest_dir='config'):

    # Launch the JVM
    jpype.startJVM(classpath=[class_path])
    from com.typesafe.config import ConfigFactory


    jar = java.util.jar.JarFile(jar_file_path)
    file = java.util.jar.JarEntry('application.conf')
    f = java.io.File(java.lang.String(dest_dir) + java.io.File.separator + file.getName())
    if not os.path.isdir(dest_dir):
        os.mkdir(dest_dir)
    input_stream = jar.getInputStream(file)
    fos = java.io.FileOutputStream(f)
    while input_stream.available() > 0:
        fos.write(input_stream.read())
    fos.close()
    input_stream.close()
    jar.close()

    config = ConfigFactory.parseFile(java.io.File(dest_dir + '/application.conf'))
    config_json = json.loads(config.toString()[26:-2])
    os.remove(dest_dir + '/application.conf')
    jpype.shutdownJVM()

    return config_json
