# Generated by Django 2.0.5 on 2018-06-12 10:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('masterdata', '0011_auto_20180612_0604'),
    ]

    operations = [
        migrations.AddField(
            model_name='ingredient',
            name='size_str',
            field=models.CharField(blank=True, max_length=250, null=True),
        ),
    ]
