from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired, Length, Optional, Email

class TrackForm(FlaskForm):
    tracking_number = StringField(
        'Tracking Number', 
        validators=[
            DataRequired(message="Tracking number is required"),
            Length(min=1, max=50, message="Tracking number must be between 1 and 50 characters")
        ]
    )
    email = StringField(
        'Email', 
        validators=[
            Optional(),
            Email(message="Please enter a valid email address"),
            Length(max=120, message="Email must be less than 120 characters")
        ]
    )
    submit = SubmitField('Track')
