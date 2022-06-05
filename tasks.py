""" 

"""
# ------------------------------------------------------- #
#                     imports
# ------------------------------------------------------- #
from invoke import task

# ------------------------------------------------------- #
#                   definitions
# ------------------------------------------------------- #
VIRTUALENV_NAME = "py39_SpeedMonitor"

DOCKER_REGISTRY = "192.168.10.21:4000"
DOCKER_IMAGE_NAME = "speed_monitor"

# ------------------------------------------------------- #
#                   global variables
# ------------------------------------------------------- #


# ------------------------------------------------------- #
#                      functions
# ------------------------------------------------------- #
def _update_requirements_txt(c):
    c.run("pip freeze > requirements.txt")

# ------------------------------------------------------- #
#                      classes
# ------------------------------------------------------- #


# ------------------------------------------------------- #
#                       tasks
# ------------------------------------------------------- #
@task
def update_requirements(c):
    with c.prefix("workon {}".format(VIRTUALENV_NAME)):
        _update_requirements_txt(c)


@task
def encode_string(c, string):
    with c.prefix("workon {}".format(VIRTUALENV_NAME)):
        from gutils.authentication_handle import encode_string_info

        encoded, result = encode_string_info(string)

        if result:
            print("could not encrypt string because -> {}".format(result))
        else:
            print("result -> {}".format(encoded))


@task
def docker_build(c, tag=""):
    print("START building docker image")
    c.run("docker build -t {img_name}{tag} .\\".format(img_name=DOCKER_IMAGE_NAME,
                                                       tag=":" + tag if tag != "" else tag))
    print("FINISHED building docker image")


@task
def docker_deployment(c, tag):
    docker_build(c, tag)
    docker_build(c)

    print("create docker tag on image and prepare for upload")
    if tag != "":
        c.run("docker tag {img_name} {registry}/{img_name}:{tag}".format(registry=DOCKER_REGISTRY,
                                                                         img_name=DOCKER_IMAGE_NAME,
                                                                         tag=tag))

    c.run("docker tag {img_name} {registry}/{img_name}".format(registry=DOCKER_REGISTRY,
                                                               img_name=DOCKER_IMAGE_NAME))

    print("going to UPLOAD image")
    if tag != "":
        c.run("docker push {registry}/{img_name}:{tag}".format(registry=DOCKER_REGISTRY,
                                                               img_name=DOCKER_IMAGE_NAME,
                                                               tag=tag))

    c.run("docker push {registry}/{img_name}".format(registry=DOCKER_REGISTRY,
                                                     img_name=DOCKER_IMAGE_NAME))

    print("delete local image")
    c.run("docker image remove {registry}/{img_name}{tag}".format(registry=DOCKER_REGISTRY,
                                                                  img_name=DOCKER_IMAGE_NAME,
                                                                  tag=":" + tag if tag != "" else tag))
    print("hopefully DEPLOYED application")
