from concurrent import futures
import logging

import grpc
import EmployeeService_pb2
import EmployeeService_pb2_grpc

import const

empDB=[
  {
    'id':101,
    'name':'Saravanan S',
    'title':'Technical Leader',
    'salary': 5000
  },
  {
    'id':201,
    'name':'Rajkumar P',
    'title':'Sr Software Engineer',
    'salary': 10000
  }
]

class EmployeeServer(EmployeeService_pb2_grpc.EmployeeServiceServicer):

    def CreateEmployee(self, request, context):
        if any(emp['id'] == request.id for emp in empDB):
            return EmployeeService_pb2.StatusReply(status='NOK')
        
        dat = {
            'id': request.id,
            'name': request.name,
            'title': request.title,
            'salary': request.salary
        }
        empDB.append(dat)
        return EmployeeService_pb2.StatusReply(status='OK')

    def GetEmployeeDataFromID(self, request, context):
        usr = [emp for emp in empDB if (emp['id'] == request.id)]

        if not usr:
            return EmployeeService_pb2.EmployeeData(
            name = 'Não existe',
            title = 'Não existe',
          )
        
        return EmployeeService_pb2.EmployeeData(
            id=usr[0]['id'],
            name=usr[0]['name'],
            title=usr[0]['title'],
            salary=usr[0]['salary']
        )

    def UpdateEmployeeTitle(self, request, context):
        usr = [emp for emp in empDB if (emp['id'] == request.id)]

        if not usr:
            return EmployeeService_pb2.StatusReply(status='NOK')

        usr[0]['title'] = request.title

        return EmployeeService_pb2.StatusReply(status='OK')

    def UpdateEmployeeSalary(self, request, context):
        usr = [emp for emp in empDB if (emp['id'] == request.id)]

        if not usr:
            return EmployeeService_pb2.StatusReply(status='NOK')

        usr[0]['salary'] = request.salary
        return EmployeeService_pb2.StatusReply(status='OK')

    def DeleteEmployee(self, request, context):
        usr = [emp for emp in empDB if (emp['id'] == request.id)]
        if not usr:
            return EmployeeService_pb2.StatusReply(status='NOK')

        empDB.remove(usr[0])
        return EmployeeService_pb2.StatusReply(status='OK')

    def ListAllEmployees(self, request, context):
        employee_list = EmployeeService_pb2.EmployeeDataList()
        for item in empDB:
            emp_data = EmployeeService_pb2.EmployeeData(
                id=item['id'],
                name=item['name'],
                title=item['title'],
                salary=item['salary']
            )
            employee_list.employee_data.append(emp_data)
        return employee_list
    
    def CalculateAverageSalary(self, request, context):
        if not empDB:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Nenhum funcionário encontrado para calcular a média de salários.")
            return EmployeeService_pb2.AverageSalaryReply(status='NOK')

        total_salary = sum(emp['salary'] for emp in empDB)
        average_salary = total_salary / len(empDB)
        average_salary = round(average_salary, 2)

        return EmployeeService_pb2.AverageSalaryReply(status='OK', average_salary=average_salary)    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    EmployeeService_pb2_grpc.add_EmployeeServiceServicer_to_server(EmployeeServer(), server)
    server.add_insecure_port('[::]:' + const.PORT)
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig()
    serve()
