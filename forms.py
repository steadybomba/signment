from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired, Length, Optional, Email

class TrackForm(FlaskForm):
    """Form for tracking shipments."""
    
    tracking_number = StringField(
        'Tracking Number', 
        validators=[
            DataRequired(message="Tracking number is required"),
            Length(min=1, max=50, message="Tracking number must be between 1 and 50 characters")
        ],
        render_kw={
            'placeholder': 'Enter your tracking number (e.g., JD1234567890)',
            'pattern': '[A-Za-z0-9]{10,20}',
            'title': 'Enter a valid tracking number (10-20 alphanumeric characters)',
            'autofocus': True
        }
    )
    
    email = StringField(
        'Email (Optional)', 
        validators=[
            Optional(),
            Email(message="Please enter a valid email address"),
            Length(max=120, message="Email must be less than 120 characters")
        ],
        render_kw={
            'placeholder': 'Enter your email for notifications',
            'type': 'email'
        }
    )
    
    submit = SubmitField('Track')

# Optional: Add a second form for admin actions if needed
class AdminShipmentForm(FlaskForm):
    """Form for admin to create/manage shipments."""
    
    tracking_number = StringField(
        'Tracking Number',
        validators=[
            DataRequired(message="Tracking number is required"),
            Length(min=10, max=50, message="Tracking number must be between 10 and 50 characters")
        ],
        render_kw={'placeholder': 'JD1234567890'}
    )
    
    status = StringField(
        'Status',
        validators=[
            DataRequired(message="Status is required"),
            Length(max=50)
        ],
        render_kw={'placeholder': 'Pending, In_Transit, Delivered, etc.'}
    )
    
    origin = StringField(
        'Origin',
        validators=[
            DataRequired(message="Origin is required"),
            Length(max=100)
        ],
        render_kw={'placeholder': 'Lagos, NG'}
    )
    
    destination = StringField(
        'Destination',
        validators=[
            DataRequired(message="Destination is required"),
            Length(max=100)
        ],
        render_kw={'placeholder': 'London, UK'}
    )
    
    recipient_email = StringField(
        'Recipient Email',
        validators=[
            Optional(),
            Email(message="Please enter a valid email address"),
            Length(max=120)
        ],
        render_kw={'placeholder': 'recipient@example.com'}
    )
    
    submit = SubmitField('Create Shipment')

# Optional: Add a search form
class SearchForm(FlaskForm):
    """Form for searching shipments."""
    
    query = StringField(
        'Search',
        validators=[
            DataRequired(message="Search query is required"),
            Length(min=1, max=100)
        ],
        render_kw={
            'placeholder': 'Search by tracking number, location...',
            'autofocus': True
        }
    )
    
    submit = SubmitField('Search')

# Optional: Add a bulk action form
class BulkActionForm(FlaskForm):
    """Form for bulk actions on shipments."""
    
    action = StringField(
        'Action',
        validators=[
            DataRequired(message="Action is required")
        ]
    )
    
    tracking_numbers = StringField(
        'Tracking Numbers',
        validators=[
            DataRequired(message="At least one tracking number is required")
        ],
        render_kw={'placeholder': 'JD1234567890, JD0987654321'}
    )
    
    submit = SubmitField('Execute')
