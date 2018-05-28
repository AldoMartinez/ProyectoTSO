#!/usr/bin/env python2.7
import yaml
import argparse
import sys
import random
import plotly.plotly as py
import plotly.figure_factory as ff
import plotly.tools
import pandas as pd
import csv
from datetime import datetime
from datetime import timedelta
class Tarea:
    """Hold information about a Tarea"""
    def __init__(self,n,eTime,pTasks):
        self.nombre = n
        self.nucleosRequeridos = 2
        self.tiempoEjecuccion = eTime
        self.tareasPadre   = pTasks # a list of strings
        self.tareasHijo = [] # a list of Task objects
        self.rutaMaxima = 0
        self.tiempoInicio = 0
        self.tiempoFinal = 0
        self.maquina = None # an instance of Machine
        self.ejecutandose = 0
        if self.rootP():
            self.preparado = 1
        else:
            self.preparado = 0

    def leafP(self):
        return not self.tareasHijo

    def rootP(self):
        return not self.tareasPadre

    def __repr__(self):
        return "<Tarea %s, coresR %s, eTime %s, pTasks %s, cTasks %s, mp %s>" % (
        self.nombre,self.nucleosRequeridos,self.tiempoEjecuccion,
        [t.nombre for t in self.tareasPadre],
        [t.nombre for t in self.tareasHijo],
        self.rutaMaxima)


class maquina:
    """Hold information about a machine"""
    def __init__(self,n):
        self.nombre = n
        self.cores = 2
        self.nucleosDisponibles = self.cores

    def __repr__(self):
        return "<Machine: nombre: %s, cores: %s, available %s>" % (self.nombre, self.cores, self.nucleosDisponibles)

class AgendacionTareas:
    """Outer class that holds all the bits of the problem, intermediate
    states, and relevant functions"""
    def __init__(self):
        self.maquinas = [] # sorted by nucleosDisponibles, least first
        self.tareas = [] # sorted by rutaMaxima, longest first
        self.tareasEjecutandose = [] # list of tareas sorted by tiempoFinal
        self.tasksDict = {} # enable fast access based on nombre
        self.agendarTareas = [] # list of (tasknombre,tiempoInicio,endTime,machineName,cores)
        self.tiempoActual = 0
        self.makeSpan = 0

    def agendar(self,archivoTareas,machinesfile):
        """Outermost function that reads in task and machines YAML files and
        then fires off the scheduling algorithm; results go in scheduleSteps"""
        self.maquinas = self.createMachines(machinesfile)
        self.maquinas.sort(key=lambda m: m.nucleosDisponibles)

        self.tareas = self.crearTareas(archivoTareas)
        self.backflow(self.tareas)
        self.tareas.sort(key=lambda t: t.rutaMaxima)
        self.tareas.reverse() # the tareas are now in a priority list based on length of critical path

        self.crearCalendario()


    def createMachines(self,filename):
        """create machines from YAML file to be resources for a schedule"""
        with open(filename, 'r') as stream:
            try:
                mData = yaml.load(stream)
                return [maquina(n) for n, c in mData.items()]
            except yaml.YAMLError as exc:
                print(exc)

    def crearTareas(self,filename):
        """create tasks from YAML file to be scheduled"""
        # with open(filename, 'r') as stream:
        #     try:
        #         tData = yaml.load(stream)
        #         print tData
        ts = [self.cTask(n,attrs) for n,attrs in filename.items()]
        self.createChildrenPointers(ts)
        return ts
            # except yaml.YAMLError as exc:
            #     print(exc)
    def cTask(self,nombre,ats):
        """helper function of crearTareas: does one task"""
        t = Tarea(nombre,ats['tiempoDeProcesamiento'],ats['tareasPadre'].keys())
        self.tasksDict[nombre] = t
        return t
    def createChildrenPointers(self,tareas):
        """file in the children field for each task"""
        for t in tareas:
            for p in t.tareasPadre:
                self.tasksDict[p].tareasHijo.append(t)
            newParents = []
            for p in t.tareasPadre:
                newParents.append(self.tasksDict[p])
            t.tareasPadre = newParents

    def backflow(self,tareas):
        """implements the backflow algorithm which labels each task with the
        longest path from it to a leaf"""
        for t in tareas:
            if t.leafP():
                t.rutaMaxima = t.tiempoEjecuccion
                self.bf(t)
    def bf(self,Tarea):
        """recursive part of backflow implementation"""
        if Tarea.rootP():
            return
        else:
            for p in Tarea.tareasPadre:
                m = Tarea.rutaMaxima + p.tiempoEjecuccion
                if m > p.rutaMaxima:
                    p.rutaMaxima = m
                self.bf(p)

    def crearCalendario(self):
        """Driver of the scheduling algorithm which starts with all
        the tareas sorted by length of longest path to a leaf from that task,
        the root tareas are marked as ready,
        and the machines sorted in ascending order by number of available cores.
        Schedule as many ready tareas in the list (put on tasksejecutandose list)
        Then a loop is executed until all the tareas are finished and removed from the list.
        The loop consists of the following steps...
          pop the first task, tf, off tasksejecutandose list (which is the next to finish)
          remove it from the tareas list
          set tiempoActual to tf.tiempoFinal
          free up cores on machine that is ejecutandose the task
          take tf off the dependencies of its children and mark them as runable if there are no more dependencies
          schedule as many ready tareas in the list
          """
        self.agendarTareasEnMaquinasDisponibles()
        while (self.tareas != []):
            tf = self.tareasEjecutandose.pop(0)
            self.tareas.remove(tf)
            self.tiempoActual = tf.tiempoFinal
            tf.maquina.nucleosDisponibles += tf.nucleosRequeridos
            for k in tf.tareasHijo:
                k.tareasPadre.remove(tf)
                if not k.tareasPadre: k.preparado = 1
            self.agendarTareasEnMaquinasDisponibles()

    def crearDiagramaGantt(self,tareas):
        with open('diagramaGantt.csv','w') as fp:
            a = csv.writer(fp,delimiter=",")
            datos = [["Task","Start","Finish","Resource"]]
            for x in tareas:
                inicio = x[1]
                fin = x[2]
                fechaInicio = str(datetime.now() + timedelta(hours=inicio))
                fechaFinal = str(datetime.now() + timedelta(hours=fin))
                aux = [x[3],fechaInicio,fechaFinal,x[0]]
                datos.append(aux)
            a.writerows(datos)

    def agendarTareasEnMaquinasDisponibles(self):
        """Schedule all the ready tareas that can be scheduled.
        loop through tareas list looking for ready tareas
        for each ready task run it if there is an available machine
          update the task, the machine, machines, tareasEjecutandose, scheduleSteps
        """
        for t in self.tareas:
            if (t.preparado and not t.ejecutandose and self.encontrarMaquina(t.nucleosRequeridos)):
                t.ejecutandose = 1
                t.maquina = self.encontrarMaquina(t.nucleosRequeridos)
                t.tiempoInicio = self.tiempoActual
                t.tiempoFinal = t.tiempoInicio + t.tiempoEjecuccion
                t.maquina.nucleosDisponibles -= t.nucleosRequeridos
                self.maquinas.sort(key=lambda m: m.nucleosDisponibles)
                self.tareasEjecutandose.append(t)
                self.tareasEjecutandose.sort(key=lambda s: s.tiempoFinal)
                self.agendarTareas.append((t.nombre,t.tiempoInicio,t.tiempoFinal,
                t.maquina.nombre))
        self.makeSpan = self.agendarTareas[-1][2]

    def encontrarMaquina(self,crequired):
        i = (m for m in self.maquinas if m.nucleosDisponibles >= crequired)
        return next(i,None)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("archivoTareas", help="Ingresa un archivo con los trabajos y tareas en formato .yaml")
        parser.add_argument("archivoMaquinas", help="Ingresa un archivo con las maquinas en formato .yaml")
        args = parser.parse_args()

        s = AgendacionTareas()
        with open(args.archivoTareas, 'r') as file:
            h = yaml.load(file)

        tareas = {}
        trabajos = h.values()
        for x in h.keys():
            if h[x]==None:
                print("Los trabajos deben de contener al menos una tarea")
                sys.exit()
        for x in range(len(h)):
            tareas.update(trabajos[x])

        while True:
            print "Forma de ingresar los tiempos de procesamiento de cada tarea"
            print "[Opcion] - [Forma]"
            print "[1] - [Aleatoriamente]"
            print "[2] - [Van incluidos en el archivo yaml]"
            n = input()
            if n != 1 and n != 2:
                print "======== Ingresa una opcion valida ========"
            else:
                # Asigna tiempos aleatorios a las tareas #
                if n == 1:
                    for x in tareas:
                        for y in tareas[x]:
                            if y == "tiempoDeProcesamiento":
                                tareas[x][y] = (random.randint(1,10))
                    break
                else:
                    break
        print '==================================================='
        s.agendar(tareas,args.archivoMaquinas)

        for x in s.agendarTareas:
            print x
        print '==================================================='
        print "MakeSpan = ",s.makeSpan
        datos = s.agendarTareas
        print "========================================"
        data = []

        s.crearDiagramaGantt(datos)
        plotly.tools.set_credentials_file(username='AldoMartinez', api_key='6aT2yIHAYNBWBdOQ18g5')
        with open('diagramaGantt.csv','rb') as f:
            reader = csv.reader(f)

        df = pd.read_csv('diagramaGantt.csv')
        fig = ff.create_gantt(df,index_col='Resource', show_colorbar=True, group_tasks=True)
        py.plot(fig, filename='gantt-use-a-pandas-dataframe', world_readable=True)
    except IndexError:
        print "Verifica que tengas bien definidas tus entradas"
    except AttributeError:
        print "Verfica la sintaxis de tus entradas"
    except KeyError:
        print "Debes de ingresar tanto el tiempo de ejecucion como las tareas padre de cada tarea, ademas verifica que existan las tareas padre que especificaste"
