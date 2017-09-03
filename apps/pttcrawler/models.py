from django.db import models
from django.utils import timezone

# Create your models here.


class Post(models.Model):
    url = models.CharField(max_length=1023, unique=True)
    author = models.CharField(max_length=63)
    title = models.CharField(max_length=1023)
    content = models.TextField()
    comment = models.TextField()
    publish_date = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return '{}'.format(self.title[:20])
