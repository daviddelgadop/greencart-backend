from rest_framework.response import Response
from rest_framework import status

class StandardResponseMixin:
    def standard_response(self, success=True, message="", data=None, errors=None, status_code=status.HTTP_200_OK):
        if success:
            return Response({
                'success': True,
                'message': message,
                'data': data
            }, status=status_code)
        else:
            return Response({
                'success': False,
                'message': message,
                'errors': errors
            }, status=status_code)
