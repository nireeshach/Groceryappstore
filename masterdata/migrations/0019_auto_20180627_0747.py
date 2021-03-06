# Generated by Django 2.0.5 on 2018-06-27 07:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('masterdata', '0018_auto_20180614_1352'),
    ]

    operations = [
        migrations.CreateModel(
            name='Pattern',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('pattern', models.TextField(max_length=100)),
                ('pattern_type', models.CharField(max_length=100)),
                ('target_pattern', models.TextField(max_length=100)),
                ('createdate', models.DateTimeField(auto_now_add=True)),
                ('lastmodified', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'Pattern',
            },
        ),
        migrations.AlterField(
            model_name='ingredient',
            name='group',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='masterdata.Group'),
        ),
    ]
