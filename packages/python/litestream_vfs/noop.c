/* Dummy C extension to force wheel platform tagging. */
#include <Python.h>

static PyMethodDef methods[] = {{NULL, NULL, 0, NULL}};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT, "_noop", NULL, -1, methods,
};

PyMODINIT_FUNC PyInit__noop(void) { return PyModule_Create(&module); }
